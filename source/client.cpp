#include <iostream>

#include <arpa/inet.h>
#include <cxxopts.hpp>
#include <fmt/format.h>

#include <message.h>
#include <shared.h>

// export PS1='\[\033[0;35m\]\u\[\033[0;33m\]$ '

// ./build/dev/clt -p 2025 -o PUT -k 10 -v 100
// ./build/dev/clt -p 2025 -o GET -k 10

auto main(int argc, char *argv[]) -> int
{
  cxxopts::Options options(argv[0], "Client for the task 3");
  options.allow_unrecognised_options().add_options()(
      "p,port", "Port of the server", cxxopts::value<in_port_t>())("o,operation",
                                                                   "Specify either a GET, PUT or DIRECT_GET operation", cxxopts::value<std::string>())(
      "k,key", "Specify key for an operation", cxxopts::value<std::string>())("v,value", "Specify value for a PUT operation", cxxopts::value<std::string>())("h,help", "Print help");

  auto args = options.parse(argc, argv);
  if (args.count("help"))
  {
    fmt::print("{}\n", options.help());
    return 0;
  }

  if (!args.count("port"))
  {
    fmt::print(stderr, "The port is required\n{}\n", options.help());
    return 1;
  }

  if (!args.count("operation"))
  {
    fmt::print(stderr, "The operation is required\n{}\n", options.help());
    return 1;
  }
  auto operation = args["operation"].as<std::string>();
  if (operation != "GET" && operation != "PUT" && operation != "DIRECT_GET")
  {
    fmt::print(stderr, "The invalid operation: {}\n", operation);
    return 1;
  }

  if (!args.count("key"))
  {
    fmt::print(stderr, "The key is required\n{}\n", options.help());
    return 1;
  }

  std::string value;
  if (!args.count("value"))
  {
    if (operation == "PUT")
    {
      fmt::print(stderr, "The value is required\n{}\n", options.help());
      return 1;
    }
  }
  else
  {
    value = args["value"].as<std::string>();
  }

  auto port = args["port"].as<in_port_t>();
  auto key = args["key"].as<std::string>();

  // TODO: implement!
  auto ret = 0;

  kvs::client_msg clt_msg;
  clt_msg.set_id(0);
  clt_msg.set_key(key);

  if (operation == "PUT")
  {
    clt_msg.set_type(kvs::client_msg_OperationType_PUT);
    clt_msg.set_value(value);
  }

  if (operation == "GET")
    clt_msg.set_type(kvs::client_msg_OperationType_GET);

  if (operation == "DIRECT_GET")
    clt_msg.set_type(kvs::client_msg_OperationType_DIRECT_GET);

  // fmt::print("[main] Type: {} [0 = PUT, 1 = GET, 2 = DIRECT_GET]..\n", clt_msg.type(), operation);

  std::string server_address = "127.0.0.1";

  int server_fd = connect_to(port, server_address, 0, 2);

  if (server_fd != -1)
  {
    // clt_msg.PrintDebugString();
    send_message(server_fd, clt_msg);

    if (!clt_msg.has_type())
      ret = 1;

    kvs::server_response svr_response;
    if (!recv_message(server_fd, &svr_response))
      ret = 2;

    switch (svr_response.status())
    {
    case kvs::server_response_Status_OK:
      if (operation == "GET" || operation == "DIRECT_GET")
        fmt::print("{}\n", svr_response.value());
      return 0;
      break;
    case kvs::server_response_Status_NOT_LEADER:
      ret = 2;
      break;
    case kvs::server_response_Status_NO_KEY:
      ret = 3;
      break;
    case kvs::server_response_Status_ERROR:
      ret = 4;
    default:
      break;
    }
    if (svr_response.status() != kvs::server_response_Status_OK)
    {
      // fmt::print("[main] Exit code: {}\n", ret);
    }
  }
  else
  {
    // fmt::print("[main] No connection");
    ret = 4;
  }

  // Exit code
  // 0 -> OK
  // 1 -> Client error
  // 2 -> Not leader
  // 3 -> No key
  // 4 -> Server error
  return ret;
}

