#include <arpa/inet.h>
#include <cxxopts.hpp>
#include <fmt/format.h>

auto main(int argc, char * argv[]) -> int {
  cxxopts::Options options("svr", "server for cloud-lab task 3");
  options.allow_unrecognised_options().add_options()(
      "p,port", "Port of the server", cxxopts::value<in_port_t>())(
      "l,leader", "Leader", cxxopts::value<bool>()->default_value("false"))(
      "r,raft_ports", "Raft ports", cxxopts::value<std::vector<in_port_t>>())(
      "i,server_id",
      "Server id",
      cxxopts::value<unsigned>()->default_value("0"))("h,help", "Print help");

  auto args = options.parse(argc, argv);

  if (args.count("help")) {
    fmt::print("{}\n", options.help());
    return 0;
  }

  if (!args.count("port")) {
    fmt::print(stderr, "The port is required\n{}\n", options.help());
    return -1;
  }

  // TODO: implement!

  return 0;
}
