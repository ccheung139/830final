#include <iostream>

#include "joiner.h"
#include "parser.h"

int main(int argc, char *argv[]) {
  Joiner joiner;

  // Read join relations
  std::string line;
  while (getline(std::cin, line)) {
    if (line == "Done") break;
    joiner.addRelation(line.c_str());
  }

  // joiner.

  // now we construct filterSelectivites

  // Preparation phase (not timed)
  // Build histograms, indexes,...

  // query => filter1, filter2, filter3
  // estimate selectivity of each filter => filterSelectivites
  // { 1: 0.2, 2: 0.8, 3: 0.1 }

  // order the joins in the query using filterSelectivites?

  

  QueryInfo i;
  while (getline(std::cin, line)) {
    if (line == "F") continue; // End of a batch
    i.parseQuery(line);
    std::cout << joiner.join(i);
  }

  return 0;
}
