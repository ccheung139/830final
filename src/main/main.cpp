#include <iostream>

#include "joiner.h"
#include "parser.h"
// #include <boost/thread.hpp>
// #include <boost/asio/io_service.hpp>

int main(int argc, char *argv[]) {
  Joiner joiner;
  // Read join relations
  std::string line;
  while (getline(std::cin, line)) {
    if (line == "Done") break;
    joiner.addRelation(line.c_str());
  }
  // joiner.numCompleted = 0;
  // boost::thread_group threadpool;
  // boost::asio::io_service ioService;
  // boost::asio::io_service::work work(ioService);
  // for (int i = 0; i < 24; ++i) {
  //   threadpool.create_thread(
  //       boost::bind(&boost::asio::io_service::run, &ioService)
  //   );
  // }

  // now we construct filterSelectivites

  // Preparation phase (not timed)
  // Build histograms, indexes,...

  // query => filter1, filter2, filter3
  // estimate selectivity of each filter => filterSelectivites
  // { 1: 0.2, 2: 0.8, 3: 0.1 }

  // order the joins in the query using filterSelectivites?

  

  QueryInfo i;
  int index = 0;
  while (getline(std::cin, line)) {
    if (line == "F") {
      for (auto& thread : joiner.threads) {
        thread.join();
      }
      // while (joiner.numCompleted != index - 1) {
      //   continue;
      // }
      for (std::string out: joiner.aggResults) {
        std::cout << out;
      }
      joiner.threads.clear();
      joiner.aggResults.clear();
      index = 0;
      continue; // End of a batch
    }
    // ioService.post(boost::bind(&Joiner::asyncJoin, &joiner, line, index));
    joiner.asyncJoin(line, index);
    ++index;
  }

  return 0;
}