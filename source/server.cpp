#include <arpa/inet.h>
#include <cxxopts.hpp>
#include <fmt/format.h>

#include <thread>

#include <shared.h>
#include <message.h>

#include <filesystem>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

in_port_t server_port;
std::vector<in_port_t> raft_ports;
std::vector<int> server_fds, client_fds;
bool isLeader = false;
bool isCandidate = false;
bool timeout_reset = false;
bool hasVoted = false; // Voted in term
int server_id;
std::string server_address = "127.0.0.1";
std::mutex m;

// export PS1='\[\033[0;35m\]\u\[\033[0;33m\]$ '

// ./build/dev/svr -r 1025,1026,1027 -i 0 -p 2025 --leader
// ./build/dev/svr -r 1025,1026,1027 -i 1 -p 2026
// ./build/dev/svr -r 1025,1026,1027 -i 2 -p 2027

//  docker exec -it [CONTAINER_ID] /bin/bash

// Test 1. Check
// python3 tests/test_replication.py --have_leader_at_startup
// Test 2.
// python3 tests/test_replication.py
// Test 3.
// python3 tests/test_replication.py --num_servers 5
// ./build/dev/svr -r 1025,1026,1027,1028,1029 -i 1 -p 2025

/*
fmt::print("{} Here 1\n", s);
fmt::print("{} Here 2\n", s);
fmt::print("{} Here 3\n", s);
fmt::print("{} Here 4\n", s);
fmt::print("{} Here 5\n", s);
*/

void print_specs();
void listen_for_server();
void listen_for_clients();

class State
{
  //(Updated on stable storage before responding to RPCs)
public:
  std::string s = "[State]";

  rocksdb::DB *state_db;

  // Persistent state on all servers:
  int currentTerm;                                   // latest term server has seen (initialized to 0 on first boot, increases monotonically)
  int votedFor;                                      // candidateId that received vote in current term (or null if none)
  std::vector<std::tuple<kvs::client_msg, int>> log; // [] log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
  // Persistent End

  // Volatile state on all servers:
  int commitIndex; // index of highest log entry known to be committed (initialized to 0, increases monotonically)
  int lastApplied; // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
  // Volatile End

  // State constructor
  explicit State(rocksdb::DB *rock_db)
  {
    currentTerm = 0;
    votedFor = -1;
    commitIndex = 0;
    lastApplied = 0;

    kvs::client_msg zero_index;
    log.push_back({zero_index, 0});
    state_db = rock_db;
    fmt::print("{} Initialized..\n", s);
  }

  // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
  int nextIndex()
  {
    return commitIndex + 1;
  }

  // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
  int matchIndex()
  {
    return lastApplied + 1;
  }
};

void construct_append_request(State *svr_state, kvs::client_msg clt_msg, raft::AppendEntriesRequest *append_request);
void construct_heartbeat(State *svr_state, raft::AppendEntriesRequest *append_request);
void handle_heartbeat(State *svr_state, raft::AppendEntriesRequest *append_request, int server_fd);
kvs::server_response get_Key(State *svr_state, kvs::client_msg clt_message);

void state_Maschine(State *svr_state)
{
  std::string s = "[state_Maschine]";
  std::string key;
  std::string value;
  rocksdb::Status status;

  fmt::print(("{} Running..\n"), s);
  while (true)
  {
    usleep(5 * 1000);

    if (!hasVoted)
      svr_state->votedFor = -1;

    if (svr_state->commitIndex > svr_state->lastApplied)
    {
      fmt::print("{} Appling message < CI{} - LA{} >\n", s, svr_state->commitIndex, svr_state->lastApplied);

      kvs::client_msg entry_tmp = get<0>(svr_state->log.at(svr_state->matchIndex()));

      key = entry_tmp.key();

      switch (entry_tmp.type())
      {
      case kvs::client_msg_OperationType_PUT:
        value = entry_tmp.value();
        status = svr_state->state_db->Put(rocksdb::WriteOptions(), key, value);
        fmt::print("{} Put executed < K{} - V{} >\n", s, key, value);
        break;
      case kvs::client_msg_OperationType_GET:
        status = svr_state->state_db->Get(rocksdb::ReadOptions(), key, &value);
        fmt::print("{} Get executed < K{} - V{} >\n", s, key, value);
        break;
      case kvs::client_msg_OperationType_DIRECT_GET:
        break;
      }
      svr_state->lastApplied++;
      fmt::print("{} Finished with {} - < CI{} - LA{} >\n\n", s, status.ToString(), svr_state->commitIndex, svr_state->lastApplied);
    }
  }
}

void send_vote_request(State *svr_state)
{
  std::string s = "[send_vote_request]";
  int server_fd = -1;
  double raft_size = (double)raft_ports.size();
  int current_port = -1;

  fmt::print("{} Running..\n", s);
  double vote_counter = 1.0;
  hasVoted = true;
  timeout_reset = true;

  for (int i = 0; i < raft_ports.size(); i++)
  {
    if (i == server_id)
    {
      // fmt::print("{} Skipping myself..\n", s);
      continue;
    }

    current_port = raft_ports.at(i);
    server_fd = connect_to(current_port, server_address, 0, 3);

    if (server_fd == -1)
    {
      continue;
    }

    // Vote request Allocation ------------------------------------------------
    /*
    raft::RequestVoteRequest *vote_request = new raft::RequestVoteRequest;

    vote_request->set_term(svr_state->currentTerm);
    vote_request->set_candidateid(server_id);
    vote_request->set_lastlogindex(svr_state->log.size());
    vote_request->set_lastlogterm(0);
    vote_request->set_lastlogterm(get<1>(svr_state->log.back()));
    */

    raft::RequestVoteRequest vote_request;

    vote_request.set_term(svr_state->currentTerm);
    vote_request.set_candidateid(server_id);
    vote_request.set_lastlogindex(svr_state->log.size());
    vote_request.set_lastlogterm(0);
    vote_request.set_lastlogterm(get<1>(svr_state->log.back()));

    // raft::TLM *send_tlm = new raft::TLM;
    raft::TLM send_tlm;
    send_tlm.set_allocated_vote_request(&vote_request);
    send_message(server_fd, &send_tlm);
    send_tlm.release_vote_request();
    // delete send_tlm;

    // Vote requests gets allocated to TLM in function ------------------------

    fmt::print("{} Vote request sent to {}..\n", s, i);

    // TLM and vote reply Memory Allocation -----------------------------------
    // fmt::print("{} TLM and vote reply Memory Allocation\n", s);
    // raft::TLM *tlm = new raft::TLM;

    raft::TLM recv_tlm;
    recv_message(server_fd, &recv_tlm);

    raft::RequestVoteReply vote_reply;
    if (recv_tlm.has_vote_reply())
    {
      vote_reply = recv_tlm.vote_reply();
      recv_tlm.release_vote_reply();

      if (vote_reply.term() == svr_state->currentTerm)
      {
        if (vote_reply.votegranted())
          vote_counter++;
      }
      else
        fmt::print("{} Reply term outdated..\n", s);
    }

    // fmt::print("{} TLM Memory Deallocation\n", s);
    // delete tlm;
    // TLM Memory Deallocated -------------------------------------------------

    close(server_fd);
  }

  fmt::print("{} {} of {} Votes CT{}\n", s, vote_counter, raft_size, svr_state->currentTerm);

  if (raft_size / 2 < vote_counter)
  {
    fmt::print("{} Is now leader..\n\n", s);
    isCandidate = false;
    isLeader = true;
  }
}

void handle_append(State *svr_state, raft::AppendEntriesRequest append_request, int server_fd)
{
  std::string s = "[handle_append]";

  // raft::TLM *tlm = new raft::TLM;
  // raft::AppendEntriesReply *entry_reply = new raft::AppendEntriesReply;

  raft::AppendEntriesReply entry_reply;
  raft::TLM send_tlm;

  int log_size = svr_state->log.size();

  // 1. Reply false if term < currentTerm (§5.1)
  if (append_request.term() < svr_state->currentTerm)
  {
    fmt::print("{} append_request.term() < svr_state->currentTerm\n", s);
    entry_reply.set_term(svr_state->currentTerm);
    entry_reply.set_success(false);

    send_tlm.set_allocated_append_reply(&entry_reply);
    send_message(server_fd, &send_tlm);
    send_tlm.release_append_reply();
    fmt::print("{} Entry reply sent..\n", s);

    // send_message(server_fd, entry_reply);
    // fmt::print("{} Clearing TLM..\n", s);
    // tlm.Clear();

    // fmt::print("{} Vote request send..\n", s);
    // send_vote_request(svr_state);
    return;
  }

  // fmt::print("{} PRE log.size\n", s);
  // fmt::print("{} log.size is {}\n", s, svr_state->log.size());

  if (append_request.prevlogindex() < log_size)
  {
    // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
    if (get<1>(svr_state->log.at(append_request.prevlogindex())) != append_request.prevlogterm())
    {
      fmt::print("{} get<1>(svr_state->log.at(append_request.prevlogindex())) != append_request.prevlogterm()\n", s);
      entry_reply.set_term(svr_state->currentTerm);
      entry_reply.set_success(false);

      send_tlm.set_allocated_append_reply(&entry_reply);
      send_message(server_fd, &send_tlm);
      send_tlm.Clear();
      fmt::print("{} Entry reply sent..\n", s);

      // send_message(server_fd, entry_reply);
      // fmt::print("{} Clearing TLM..\n", s);

      return;
    }

    // 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
    for (int i = 0; i < log_size - 1; i++)
    {
      if (i == append_request.prevlogindex() + 1 && get<1>(svr_state->log.at(i)) != append_request.term())
      {
        fmt::print("{} 3. entry conflict #TODO: delete the existing entry and all that follow it (§5.3)\n", s);
      }
    }
  }

  // 4. Append any new entries not already in the log
  // fmt::print("{} # {} # svr_state->log.size() < # {} # append_request.prevlogindex()\n", s, svr_state->log.size(), append_request.prevlogindex());
  if (log_size <= append_request.prevlogindex() && append_request.entries_size() != 0) // append_request.entries_size() != 0 &&
  {
    // fmt::print("{} Message: \n{}\n\n", s, append_request.DebugString());
    svr_state->log.push_back({append_request.entries(0).msg(), append_request.term()});
    fmt::print("{} Logged entry < CT{} - NI{} >\n", s, append_request.term(), svr_state->nextIndex());
  }

  // fmt::print("{} LeaderCommit: {} current commitIndex: {} \n", s, append_request.leadercommit(), svr_state->commitIndex);
  // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
  if (append_request.leadercommit() > svr_state->commitIndex)
  {
    // fmt::print("{} append_request.leadercommit() > svr_state->commitIndex\n", s);
    svr_state->commitIndex = std::min(append_request.leadercommit(), svr_state->log.size());
  }

  entry_reply.set_term(svr_state->currentTerm);
  entry_reply.set_success(true);

  send_tlm.set_allocated_append_reply(&entry_reply);
  send_message(server_fd, &send_tlm); // &entry_reply);
  send_tlm.release_append_reply();
  // delete entry_reply;

  // fmt::print("{} Finished..\n\n", s);
}

void handle_vote(State *svr_state, raft::RequestVoteRequest vote_request, int server_fd)
{
  std::string s = "[handle_vote]";

  fmt::print("{} Running..\n", s);

  if (svr_state->currentTerm < vote_request.term())
  {
    svr_state->currentTerm = vote_request.term();
    hasVoted = false;
  }
  else
  {
    fmt::print("{} Has already voted TC{}..\n", s, svr_state->currentTerm);
  }

  // 1. Reply false if term < currentTerm (§5.1)
  bool vote_1 = !(vote_request.term() < svr_state->currentTerm);
  // 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
  bool vote_2 = (svr_state->votedFor == -1 || svr_state->votedFor == vote_request.candidateid());
  bool vote_3 = (vote_request.lastlogindex() >= svr_state->commitIndex);

  bool vote_final = vote_1 && (vote_2 || vote_3) && !hasVoted && !isCandidate;

  if (vote_final)
  {
    svr_state->votedFor = vote_request.candidateid();
    hasVoted = true;
  }

  // Vote reply Allocation ----------------------------------------------------
  // raft::RequestVoteReply *vote_reply = new raft::RequestVoteReply;
  raft::RequestVoteReply vote_reply;

  vote_reply.set_term(svr_state->currentTerm);
  vote_reply.set_votegranted(vote_final);

  raft::TLM send_tlm;
  send_tlm.set_allocated_vote_reply(&vote_reply);
  send_message(server_fd, &send_tlm);
  send_tlm.release_vote_reply();
  // Vote reply gets allocated to TLM in function --------------------------

  fmt::print("{} Voted ", s);
  fmt::print("{} for {} TC{}..\n\n", vote_final, vote_request.candidateid(), svr_state->currentTerm);
}

void handle_server(State *svr_state)
{
  std::string s = "[handle_server]";

  while (true)
  {
    int server_fd;
    // timeval timeout = {5, 0};
    raft::TLM tlm;
    // raft::AppendEntriesRequest append_request;
    // raft::RequestVoteRequest vote_request;

    char tmp[1] = "";
    usleep(5 * 1000);

    while (!server_fds.empty())
    {
      server_fd = server_fds.back();
      server_fds.pop_back();

      // TLM Allocation -------------------------------------------------------
      // raft::TLM tlm;

      recv_message(server_fd, &tlm);

      // fmt::print("{} TLM Message:\n{}\n", s, tlm.DebugString());

      timeout_reset = true;

      if (tlm.has_append_request())
      {
        // fmt::print("{} Append request..\n", s);
        handle_append(svr_state, tlm.append_request(), server_fd);
      }

      if (tlm.has_vote_request())
      {
        // fmt::print("{} Vote request..\n", s);
        if (!isLeader)
        {
          handle_vote(svr_state, tlm.vote_request(), server_fd);
        }
      }

      // fmt::print("{} TLM Memory Deallocation\n", s);
      // delete tlm;
      // TLM Memory Deallocated -------------------------------------------------

      close(server_fd);
    }
  }
}

void handle_clients(State *svr_state)
{
  std::string s = "[handle_clients]";

  double success_counter = 1.0;
  double raft_size = (double)raft_ports.size();
  int server_fd;
  int server_port;
  bool majority_succuss = false;

  raft::TLM send_tlm;
  raft::AppendEntriesRequest append_request;
  raft::TLM recv_tlm;

  int client_fd;
  while (true)
  {
    usleep(5 * 1000);
    if (!client_fds.empty())
    {
      // fmt::print("\n{} Got - ", s);
      client_fd = client_fds.back();
      client_fds.pop_back();
      // fmt::print("{} - \n", client_fd);

      kvs::client_msg *clt_message =  new kvs::client_msg;
      kvs::server_response server_response;
      // kvs::client_msg *clt_message = new kvs::client_msg;

      recv_message(client_fd, clt_message);
      // fmt::print("{} Clt msg: \n{}\n", s, clt_message->DebugString());

      if (clt_message->type() == kvs::client_msg_OperationType_DIRECT_GET)
      {
        server_response = get_Key(svr_state, *clt_message);
        // fmt::print("{} Reponse:\n{}\n\n", s, server_response.DebugString());
        send_message(client_fd, server_response);
        close(client_fd);

        // delete clt_message;
        continue;
      }

      if (!isLeader)
      {
        server_response.set_id(server_id);
        server_response.set_status(kvs::server_response_Status_NOT_LEADER);
        // fmt::print("{} Reponse:\n{}\n\n", s, server_response.DebugString());
        send_message(client_fd, server_response);
        close(client_fd);

        // delete clt_message;
        continue;
      }

      if (clt_message->type() == kvs::client_msg_OperationType_GET)
      {
        server_response = get_Key(svr_state, *clt_message);
        // fmt::print("{} Reponse:\n{}\n\n", s, server_response.DebugString());
        send_message(client_fd, server_response);

        close(client_fd);
        // delete clt_message;
        continue;
      }

      if (clt_message->type() == kvs::client_msg_OperationType_PUT)
      {
        svr_state->log.push_back({*clt_message, svr_state->currentTerm});
        success_counter = 1;
        fmt::print("{} Logged entry < CT{} - NI{} >\n", s, svr_state->currentTerm, svr_state->nextIndex());

        // raft::AppendEntriesRequest *append_request = new raft::AppendEntriesRequest;
        // construct_append_request(svr_state, clt_message, &append_request);

        
        construct_heartbeat(svr_state, &append_request);
        append_request.add_entries()->set_allocated_msg(clt_message);
        send_tlm.set_allocated_append_request(&append_request);

        // fmt::print("{} TLM Message: \n{}\n\n", s, send_tlm.DebugString());

        for (int i = 0; i < raft_ports.size(); i++)
        {

          if (i == server_id)
            continue;

          server_port = raft_ports.at(i);

          server_fd = connect_to(server_port, server_address, 0, 0);

          if (server_fd == -1)
          {
            fmt::print("{} {} not reachable\n", s, server_port);
            continue;
          }

          // fmt::print("{} Send_tlm: \n{}\n\n", s, append_request.DebugString());
          send_message(server_fd, &send_tlm);

          // raft::TLM *tlm = new raft::TLM;

          recv_message(server_fd, &recv_tlm);

          if (recv_tlm.has_append_reply())
          {
            auto append_reply = recv_tlm.append_reply();
            recv_tlm.release_append_reply();

            if (append_reply.term() > svr_state->currentTerm)
            {
              svr_state->currentTerm = append_reply.term();
              isLeader = false;
              hasVoted = false;
              fmt::print("{} Follower's ({}) term is higher {} > TC{}..\n", s, server_fd, append_reply.term(), svr_state->currentTerm);
            }
            if (append_reply.success())
              success_counter++;
          }
          // delete tlm;
        }

        send_tlm.release_append_request();

        majority_succuss = (raft_size / 2 < success_counter);

        fmt::print("{} {} of {} were successful\n", s, success_counter, raft_size);
        server_response.set_id(server_id);

        if (majority_succuss)
        {
          svr_state->commitIndex++;
          // fmt::print("{} CI{}\n", s, svr_state->commitIndex);
          server_response.set_status(kvs::server_response_Status_OK);
        }
        else
        {
          server_response.set_status(kvs::server_response_Status_ERROR);
        }

        // fmt::print("{} Response: \n{}", s, server_response.DebugString());
        send_message(client_fd, server_response);
        
        close(client_fd);
      }

      fmt::print("{} Finished.. CI{}\n\n", s, svr_state->commitIndex);
      // server_response.Clear();
      // clt_message.Clear();
    }
  }
}

void heartbeat(State *svr_state, int time)
{
  std::string s = "[heartbeat]";
  fmt::print("{} Running with {} ms..\n", s, time);
  int server_fd = 0;
  int current_raft_port;

  while (true)
  {
    usleep(time * 1000);

    if (isLeader)
    {
      isCandidate = false;

      // fmt::print("{} Sending heartbeat..\n", s);
      for (int i = 0; i < raft_ports.size(); i++)
      {
        if (i == server_id)
        {
          // fmt::print("{} Skipping myself..\n", s);
          continue;
        }

        // fmt::print("{} Server_id {} from {}..\n", s, i, raft_ports.size() - 1);
        current_raft_port = raft_ports.at(i);
        // fmt::print("{} Connect to server on {}\n", s, server_port);
        server_fd = connect_to(current_raft_port, server_address, 0, 3);

        if (server_fd == -1)
        {
          if (time > 1000)
            fmt::print("{} Server {} NOT OK..\n", s, i);
          continue;
        }

        // raft::AppendEntriesRequest *append_request = new raft::AppendEntriesRequest;
        raft::AppendEntriesRequest append_request;
        construct_heartbeat(svr_state, &append_request);

        raft::TLM send_tlm;
        send_tlm.set_allocated_append_request(&append_request);
        send_message(server_fd, &send_tlm); // append_request);
        send_tlm.release_append_request();
        // delete append_request;

        // raft::TLM *tlm = new raft::TLM;
        raft::TLM recv_tlm;
        recv_message(server_fd, &recv_tlm);

        raft::AppendEntriesReply append_reply;
        if (recv_tlm.has_append_reply())
        {
          append_reply = recv_tlm.append_reply();
          recv_tlm.release_append_reply();

          if (!append_reply.has_term() || !append_reply.success()) // if (!append_reply.has_term() || !append_reply.success())
          {
            fmt::print("{} Reply from {} was failure..\n", s, server_port);
            continue;
          }

          if (svr_state->currentTerm < append_reply.term()) // append_reply.term())
          {
            if (isLeader)
            {
              fmt::print("{} Follower term > current Term - not Leader anymore..\n", s);
              isLeader = false;
              hasVoted = false;
              svr_state->currentTerm = append_reply.term();
              break;
            }
          }
        }

        append_request.Clear();
        send_tlm.Clear();
        recv_tlm.Clear();
        append_reply.Clear();

        if (time > 1000)
          fmt::print("{} Server {} OK..\n", s, i);
      }
      // fmt::print("\n", s);
    }
  }
}

void election_timeout(State *svr_state, int min, int max)
{
  std::string s = "[election_timeout]";
  fmt::print("{} Running with {} - {} ms..\n\n", s, min, max);
  int rand_sleep;
  std::srand(time(NULL));

  while (true)
  {
    if (!isLeader)
    {
      rand_sleep = (min + (std::rand() % (max - min + 1))) * 1000;
      if (min > 10000)
        fmt::print("{} Sleep for random {} ms..\n", s, rand_sleep / 1000);

      usleep(rand_sleep);

      if (!isLeader && !timeout_reset)
      {
        isCandidate = true;
        svr_state->currentTerm++;

        fmt::print("{} Timed out CT{}..\n", s, svr_state->currentTerm);
        send_vote_request(svr_state); // Timeout reset in function when "Running"
      }
      else
      {
        timeout_reset = false;
      }
    }
  }
}

auto main(int argc, char *argv[]) -> int
{
  cxxopts::Options options("svr", "server for cloud-lab task 3");
  options.allow_unrecognised_options().add_options()(
      "p,port", "Port of the server", cxxopts::value<in_port_t>())(
      "l,leader", "Leader", cxxopts::value<bool>()->default_value("false"))(
      "r,raft_ports", "Raft ports", cxxopts::value<std::vector<in_port_t>>())(
      "i,server_id", "Server id", cxxopts::value<unsigned>()->default_value("0"))("h,help", "Print help");

  auto args = options.parse(argc, argv);

  if (args.count("help"))
  {
    fmt::print("{}\n", options.help());
    return 0;
  }

  if (!args.count("port"))
  {
    fmt::print(stderr, "The port is required\n{}\n", options.help());
    return -1;
  }

  server_port = args["port"].as<in_port_t>();
  isLeader = args["leader"].as<bool>();
  raft_ports = args["raft_ports"].as<std::vector<in_port_t>>();
  server_id = args["server_id"].as<unsigned>();

  print_specs();

  // rocksDB stuff
  rocksdb::DB *rock_db;
  rocksdb::Options rock_options;
  rocksdb::Status rock_s;

  rock_options.IncreaseParallelism();
  rock_options.OptimizeLevelStyleCompaction();
  rock_options.compression_per_level.resize(rock_options.num_levels);
  rock_options.create_if_missing = true;

  for (int i = 0;
       i < rock_options.num_levels; i++)
  {
    rock_options.compression_per_level[i] = rocksdb::kNoCompression;
  }

  rock_options.compression = rocksdb::kNoCompression;

  std::string root;
  std::filesystem::path cwd = std::filesystem::current_path();
  std::string main = "/rockDBs/sub_DB_";

  int sub_dir_no;
  for (int i = 0;; i++)
  {
    root = cwd.c_str() + main + std::to_string(i);
    rock_s = rocksdb::DB::Open(rock_options, root, &rock_db);
    if (!rock_s.IsIOError())
    {
      sub_dir_no = i;
      break;
    }
  }
  fmt::print("RockDB is {} at {}{}\n\n", rock_s.ToString().c_str(), main.c_str(), sub_dir_no);
  assert(rock_s.ok());
  // rocksDB stuff End

  State svr_state(rock_db);
  std::vector<std::thread> threads;

  if (isLeader)
    svr_state.currentTerm++;

  threads.emplace_back(listen_for_clients);
  usleep(5 * 1000 * 10);
  threads.emplace_back(listen_for_server);
  usleep(5 * 1000 * 10);
  threads.emplace_back(state_Maschine, &svr_state);
  usleep(5 * 1000 * 10);
  threads.emplace_back(heartbeat, &svr_state, 150); // Sleep in (15) ms
  usleep(5 * 1000 * 10);
  threads.emplace_back(election_timeout, &svr_state, 1500, 3000); // Sleep in 150 - 300 ms

  threads.emplace_back(handle_clients, &svr_state);
  threads.emplace_back(handle_server, &svr_state);

  for (auto &thread : threads)
  {
    thread.join();
  }

  fmt::print("** all threads joined **\n");

  return 0;
}

void listen_for_server()
{
  fmt::print("[listen_for_server] Running..\n");
  int my_port = raft_ports.at(server_id);
  while (true)
  {
    usleep(5 * 1000);
    accept_connections(my_port, &server_fds, 0, 0);
  }
}

void listen_for_clients()
{
  fmt::print("[listen_for_clients] Running..\n");
  while (true)
  {
    usleep(5 * 1000 * 100);
    accept_connections(server_port, &client_fds, 0, 0);
  }
}

void construct_append_request(State *svr_state, kvs::client_msg *clt_msg, raft::AppendEntriesRequest *append_request)
{
  std::string s = "[construct_append_request]";

  // fmt::print("{} Entry size: {}\n", s, append_request->entries_size());
  // append_request->clear_entries();

  append_request->set_term(svr_state->currentTerm);
  append_request->set_leaderid(server_id);

  int prev_log = svr_state->log.size() - 1;
  append_request->set_prevlogindex(prev_log);
  append_request->set_prevlogterm(get<1>(svr_state->log.at(prev_log)));

  // fmt::print("{} Clt: \n{}\n", s, clt_msg->DebugString());
  // fmt::print("{} Entry size: {}\n", s, append_request->entries_size());
  raft::Entry *entry = append_request->add_entries();
  entry->set_allocated_msg(clt_msg);

  append_request->set_leadercommit(svr_state->commitIndex);

  fmt::print("{} Message: \n{}\n", s, append_request->DebugString());
}

void construct_heartbeat(State *svr_state, raft::AppendEntriesRequest *append_request)
{
  append_request->Clear();

  append_request->set_term(svr_state->currentTerm);
  append_request->set_leaderid(server_id);

  int prev_log = svr_state->log.size() - 1;
  append_request->set_prevlogindex(prev_log);
  append_request->set_prevlogterm(get<1>(svr_state->log.at(prev_log)));
  append_request->set_leadercommit(svr_state->commitIndex);

  // fmt::print("[construct_heartbeat] Message: \n{}\n", append_request->DebugString());
}

kvs::server_response get_Key(State *svr_state, kvs::client_msg clt_message)
{
  kvs::server_response svr_response;

  svr_response.set_id(server_id);
  std::string value;
  rocksdb::Status status = svr_state->state_db->Get(rocksdb::ReadOptions(), clt_message.key(), &value);
  if (status.ok())
  {
    svr_response.set_status(kvs::server_response_Status_OK);
    svr_response.set_value(value);
  }
  else
  {
    svr_response.set_status(kvs::server_response_Status_NO_KEY);
  }

  return svr_response;
}

void print_specs()
{
  fmt::print("-----------------------------------\n");
  fmt::print("CT: Current Term, NI: Next Index\n");
  fmt::print("CI: Current Index, LA: Last Applied\n");
  int ports = raft_ports.size();
  fmt::print("server_port: {}\nisleader: {}\nraft_ports: {} - [", server_port, isLeader, ports);
  for (int i = 0; i < ports; i++)
  {
    fmt::print("{}", raft_ports.at(i));
    if (i != ports - 1)
      fmt::print(", ");
  }
  fmt::print("]\nserver_id: {}\n", server_id);
  fmt::print("-----------------------------------\n\n");
}
