#pragma once

#include <vector>
#include <cstdint>
#include <set>

#include "operators.h"
#include "relation.h"
#include "parser.h"

class Joiner
{
private:
  /// The relations that might be joined
  std::vector<Relation> relations_;

  // std::vector<FilterInfo> filters_copy;

public:
  std::vector<std::string> aggResults;
  std::vector<std::thread> threads;
  /// Add relation
  void addRelation(const char *file_name);
  void addRelation(Relation &&relation);
  static void appendHistogram(std::vector<std::vector<int>> histogram);
  static void appendRelationSize(uint64_t size);
  double estimateSelectivity(std::vector<int> histogram, uint64_t minVal, uint64_t maxVal, int bucketWidth, FilterInfo::Comparison op, uint64_t val, int nTups);
  /// Get relation
  const Relation &getRelation(unsigned relation_id);
  /// Joins a given set of relations
  std::string join(std::string line, int index);

  void asyncJoin(std::string line, int index);

  const std::vector<Relation> &relations() const { return relations_; }

private:
  /// Add scan to query
  std::shared_ptr<Operator> addScan(std::set<unsigned> &used_relations,
                                    const SelectInfo &info,
                                    QueryInfo &query);
  
  double isFilterScan(const SelectInfo &info, QueryInfo &query);
};
