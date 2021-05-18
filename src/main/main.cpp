#include <iostream>

#include "joiner.h"
#include "parser.h"

int main(int argc, char *argv[])
{
  Joiner joiner;

  // Read join relations
  std::string line;
  while (getline(std::cin, line))
  {
    if (line == "Done")
      break;

    joiner.addRelation(line.c_str());
  }

  auto relations = &joiner.relations();
  std::vector<std::tuple<std::vector<std::map<uint64_t, std::vector<uint64_t>>>, std::vector<std::vector<std::vector<uint64_t>>>>> relationToHashTable;

  for (unsigned i = 0; i < relations->size(); i++)
  {
    std::tuple<std::vector<std::map<uint64_t, std::vector<uint64_t>>>, std::vector<std::vector<std::vector<uint64_t>>>> hashTableAndSortedVals =
        relations->at(i).performRelationWork();
    relationToHashTable.push_back(hashTableAndSortedVals);
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
  while (getline(std::cin, line))
  {
    if (line == "F")
      continue; // End of a batch
    i.parseQuery(line);

    std::vector<FilterInfo> filterForGivenRelation = i.filters;

    
    std::cout << joiner.join(i);
  }

  return 0;
}
