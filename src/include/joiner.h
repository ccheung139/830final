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

  std::vector<FilterInfo> filters_copy;

public:
  /// Add relation
  void addRelation(const char *file_name);
  void addRelation(Relation &&relation);
  static void appendHistogram(std::vector<std::vector<int>> histogram);
  double estimateSelectivity(std::vector<int> histogram, uint64_t minVal, uint64_t maxVal, int bucketWidth, FilterInfo::Comparison op, uint64_t val, int nTups);
  /// Get relation
  const Relation &getRelation(unsigned relation_id);
  /// Joins a given set of relations
  std::string join(QueryInfo &i);

  const std::vector<Relation> &relations() const { return relations_; }

private:
  /// Add scan to query
  std::unique_ptr<Operator> addScan(std::set<unsigned> &used_relations,
                                    const SelectInfo &info,
                                    QueryInfo &query);
  
  double isFilterScan(const SelectInfo &info, QueryInfo &query);
};
