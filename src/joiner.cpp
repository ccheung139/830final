#include "joiner.h"

#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <set>
#include <sstream>
#include <vector>

#include "parser.h"

namespace
{

enum QueryGraphProvides
{
  Left,
  Right,
  Both,
  None
};

// Analyzes inputs of join
QueryGraphProvides analyzeInputOfJoin(std::set<unsigned> &usedRelations,
                                      SelectInfo &leftInfo,
                                      SelectInfo &rightInfo)
{
  bool used_left = usedRelations.count(leftInfo.binding);
  bool used_right = usedRelations.count(rightInfo.binding);

  if (used_left ^ used_right)
    return used_left ? QueryGraphProvides::Left : QueryGraphProvides::Right;
  if (used_left && used_right)
    return QueryGraphProvides::Both;
  return QueryGraphProvides::None;
}

} // namespace

std::vector<std::vector<std::vector<int>>> histogramList;

void Joiner::appendHistogram(std::vector<std::vector<int>> histogram)
{
  histogramList.push_back(histogram);
}

// Loads a relation_ from disk
void Joiner::addRelation(const char *file_name)
{
  relations_.emplace_back(file_name);
}

void Joiner::addRelation(Relation &&relation)
{
  relations_.emplace_back(std::move(relation));
}

// Loads a relation from disk
const Relation &Joiner::getRelation(unsigned relation_id)
{
  if (relation_id >= relations_.size())
  {
    std::cerr << "Relation with id: " << relation_id << " does not exist"
              << std::endl;
    throw;
  }
  return relations_[relation_id];
}

// Add scan to query
std::unique_ptr<Operator> Joiner::addScan(std::set<unsigned> &used_relations,
                                          const SelectInfo &info,
                                          QueryInfo &query)
{
  used_relations.emplace(info.binding);
  std::vector<FilterInfo> filters;
  for (auto &f : query.filters())
  {
    if (f.filter_column.binding == info.binding)
    {
      filters.emplace_back(f);
    }
  }
  return !filters.empty() ? std::make_unique<FilterScan>(getRelation(info.rel_id), filters)
                          : std::make_unique<Scan>(getRelation(info.rel_id),
                                                   info.binding);
}

// Executes a join query
std::string Joiner::join(QueryInfo &query)
{
  std::set<unsigned> used_relations;

  // We always start with the first join predicate and append the other joins
  // to it (--> left-deep join trees). You might want to choose a smarter
  // join ordering ...

  // enum Comparison : char { Less = '<', Greater = '>', Equal = '=' };

  std::cerr << histogramList[0].size() << std::endl;
  // auto relation_ids = query.relation_ids();
  // for (unsigned i = 0; i < relation_ids.size(); ++i)
  // {
  //   auto &relation_id = relation_ids[i];
  //   std::cerr << "relationId: " << relation_id << std::endl;
  //   std::vector<std::vector<int>> &histograms = histogramList[relation_id];

  //   std::vector<int> &hist0 = histograms[0];

  //   for (int i = 0; i < hist0.size(); i++)
  //   {
  //     std::cout << "Frequency for bucket " << i << " : " << hist0[i] << std::endl;
  //   }
  // }

  auto filters_copy = query.filters();
  for (unsigned i = 0; i < filters_copy.size(); ++i)
  {
    auto &indiv_filter = filters_copy[i];
    FilterInfo::Comparison comparison = indiv_filter.comparison;
    auto constant = indiv_filter.constant;
    SelectInfo select_info = indiv_filter.filter_column;
    RelationId relation_id = select_info.rel_id;
    unsigned col_id = select_info.col_id;

    std::vector<std::vector<int>> &histograms = histogramList[relation_id];
    std::vector<int> &histogram = histograms[col_id];

    int nTups = histogram.back();
    histogram.pop_back();
    int bucketWidth = histogram.back();
    histogram.pop_back();
    int maxVal = histogram.back();
    histogram.pop_back();
    int minVal = histogram.back();
    histogram.pop_back();
    double selectivity = estimateSelectivity(histogram, minVal, maxVal, bucketWidth, comparison, constant, nTups);
    std::cerr << "selectivity: " << selectivity << std::endl;

    histogram.push_back(minVal);
    histogram.push_back(maxVal);
    histogram.push_back(bucketWidth);
    histogram.push_back(nTups);

    // now run estimate selectivity

    // estimateSelectivity on indiv_filter
    //  std::cerr << "constant: " << std::endl;
    // std::cerr << constant << std::endl;
  }

  // make filter selectivities
  // make join ordering

  // std::cout << query.dumpText() << std::endl;

  //query.predicates()
  //query.filters()

  // auto predicates_copy2 = query.predicates();
  // std::cout << predicates_copy2.size() << std::endl;
  // for (unsigned i = 0; i < predicates_copy2.size(); ++i)
  // {
  //   auto &p_info2 = predicates_copy2[i];
  //   std::cout << "start here: " << std::endl;
  //   std::cout << p_info2.dumpText() << std::endl;
  // }

  const auto &firstJoin = query.predicates()[0];
  std::unique_ptr<Operator> left, right;
  left = addScan(used_relations, firstJoin.left, query);
  right = addScan(used_relations, firstJoin.right, query);
  std::unique_ptr<Operator>
      root = std::make_unique<Join>(move(left), move(right), firstJoin);

  auto predicates_copy = query.predicates();
  for (unsigned i = 1; i < predicates_copy.size(); ++i)
  {
    auto &p_info = predicates_copy[i];
    auto &left_info = p_info.left;
    auto &right_info = p_info.right;

    switch (analyzeInputOfJoin(used_relations, left_info, right_info))
    {
    case QueryGraphProvides::Left:
      left = move(root);
      right = addScan(used_relations, right_info, query);
      root = std::make_unique<Join>(move(left), move(right), p_info);
      break;
    case QueryGraphProvides::Right:
      left = addScan(used_relations,
                     left_info,
                     query);
      right = move(root);
      root = std::make_unique<Join>(move(left), move(right), p_info);
      break;
    case QueryGraphProvides::Both:
      // All relations of this join are already used somewhere else in the
      // query. Thus, we have either a cycle in our join graph or more than
      // one join predicate per join.
      root = std::make_unique<SelfJoin>(move(root), p_info);
      break;
    case QueryGraphProvides::None:
      // Process this predicate later when we can connect it to the other
      // joins. We never have cross products.
      predicates_copy.push_back(p_info);
      break;
    };
  }

  Checksum checksum(move(root), query.selections());
  checksum.run();

  std::stringstream out;
  auto &results = checksum.check_sums();
  for (unsigned i = 0; i < results.size(); ++i)
  {
    out << (checksum.result_size() == 0 ? "NULL" : std::to_string(results[i]));
    if (i < results.size() - 1)
      out << " ";
  }
  out << "\n";
  return out.str();
}

double Joiner::estimateSelectivity(std::vector<int> histogram, int minVal, int maxVal, int bucketWidth, FilterInfo::Comparison op, uint64_t val, int nTups)
{
  std::cerr << "OPERATION: " << char(op) << std::endl;

  int NUM_BUCKETS = 10;
  int bucketIndex = std::min(int((val - minVal) / bucketWidth), NUM_BUCKETS - 1);

  switch (op)
  {
  case FilterInfo::Comparison::Equal:
  {
    if (val < minVal || val > minVal)
    {
      return 0.0;
    }
    return 1.0 * histogram[bucketIndex] / bucketWidth / nTups;
  }
  case FilterInfo::Comparison::Less:
  {
    if (val > maxVal)
    {
      return 1.0;
    }
    if (val < minVal)
    {
      return 0.0;
    }
    int bucketMin = minVal + bucketWidth * bucketIndex;
    double proportionLessThan = (val - (float)bucketMin) / (float)bucketWidth;
    double runningSum =
        proportionLessThan *
        (1.0 * histogram[bucketIndex] / (float)nTups);

    std::cerr << "FIRST RUNNING SUM : " << runningSum << " BUCKET INDEX: " << bucketIndex << " BUCKET MIN " << bucketMin << " BUCKET WIDTH " << bucketWidth << std::endl;

    for (int i = 0; i < bucketIndex; i++)
    { 
      runningSum += 1.0 * histogram[i] / nTups;
    }
    return runningSum;
  }
  case FilterInfo::Comparison::Greater:
  {
    return 1 - estimateSelectivity(histogram, minVal, maxVal, bucketWidth, FilterInfo::Comparison::Less, val, nTups) - estimateSelectivity(histogram, minVal, maxVal, bucketWidth, FilterInfo::Comparison::Equal, val, nTups);
  }
  default:
  {
    return 0.0;
  }
  }
}
