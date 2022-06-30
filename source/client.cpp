#include <iostream>

#include <arpa/inet.h>
#include <cxxopts.hpp>
#include <fmt/format.h>

auto main(int argc, char * argv[]) -> int {
  cxxopts::Options options(argv[0], "Client for the task 3");
  options.allow_unrecognised_options().add_options()(
      "p,port", "Port of the server", cxxopts::value<in_port_t>())(
      "o,operation",
      "Specify either a GET, PUT or DIRECT_GET operation",
      cxxopts::value<std::string>())(
      "k,key", "Specify key for an operation", cxxopts::value<std::string>())(
      "v,value",
      "Specify value for a PUT operation",
      cxxopts::value<std::string>())("h,help", "Print help");

  auto args = options.parse(argc, argv);
  if (args.count("help")) {
    fmt::print("{}\n", options.help());
    return 0;
  }

  if (!args.count("port")) {
    fmt::print(stderr, "The port is required\n{}\n", options.help());
    return 1;
  }

  if (!args.count("operation")) {
    fmt::print(stderr, "The operation is required\n{}\n", options.help());
    return 1;
  }
  auto operation = args["operation"].as<std::string>();
  if (operation != "GET" && operation != "PUT" && operation != "DIRECT_GET") {
    fmt::print(stderr, "The invalid operation: {}\n", operation);
    return 1;
  }

  if (!args.count("key")) {
    fmt::print(stderr, "The key is required\n{}\n", options.help());
    return 1;
  }

  std::string value;
  if (!args.count("value")) {
    if (operation == "PUT") {
      fmt::print(stderr, "The value is required\n{}\n", options.help());
      return 1;
    }
  } else {
    value = args["value"].as<std::string>();
  }

  auto port = args["port"].as<in_port_t>();
  auto key = args["key"].as<std::string>();

  // TODO: implement!
  auto ret = 0;

  // Exit code
  // 0 -> OK
  // 1 -> Client error
  // 2 -> Not leader
  // 3 -> No key
  // 4 -> Server error
  return ret;
}
