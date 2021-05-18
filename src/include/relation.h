#pragma once

#include <map>
#include <cstdint>
#include <string>
#include <vector>

using RelationId = unsigned;

class Relation
{
private:
  /// Owns memory (false if it was mmaped)
  bool owns_memory_;
  /// The number of tuples
  uint64_t size_;
  /// The join column containing the keys
  std::vector<uint64_t *> columns_;

  // std::tuple<std::vector<std::map<uint64_t, std::vector<uint64_t>>>, std::vector<std::vector<std::vector<uint64_t>>>> &hashTableAndSortedVals_;

public:
  std::tuple<std::vector<std::map<uint64_t, std::vector<uint64_t>>>, std::vector<std::vector<std::vector<uint64_t>>>> &hashTableAndSortedVals_;

  /// Constructor without mmap
  Relation(uint64_t size, std::vector<uint64_t *> &&columns)
      : owns_memory_(true), size_(size), columns_(columns)
  {
  }
  /// Constructor using mmap
  explicit Relation(const char *file_name);
  /// Delete copy constructor
  Relation(const Relation &other) = delete;
  /// Move constructor
  Relation(Relation &&other) = default;

  /// The destructor
  ~Relation();

  /// Stores a relation into a file (binary)
  void storeRelation(const std::string &file_name);

  // int *minValPtr;
  // int *maxValPtr;
  // int *bucketWidthPtr;

  /// Stores a relation into a file (csv)
  void storeRelationCSV(const std::string &file_name);
  /// Dump SQL: Create and load table (PostgreSQL)
  void dumpSQL(const std::string &file_name, unsigned relation_id);

  /// The number of tuples
  uint64_t size() const { return size_; }
  /// The join column containing the keys
  const std::vector<uint64_t *> &columns() const { return columns_; }

  // const std::tuple<std::vector<std::map<uint64_t, std::vector<uint64_t>>>, std::vector<std::vector<std::vector<uint64_t>>>> &getHashStuff() const
  // {
  //   return hashStuff;
  // }

  const std::tuple<std::vector<std::map<uint64_t, std::vector<uint64_t>>>, std::vector<std::vector<std::vector<uint64_t>>>> performRelationWork() const;

  // const std::tuple<std::vector<std::map<uint64_t, std::vector<uint64_t>>>, std::vector<std::vector<std::vector<uint64_t>>>>
  //     &hashTableAndSortedVals() const { return hashTableAndSortedVals_; }

private:
  std::vector<int> hashStuff_;

  /// Loads data from a file
  void loadRelation(const char *file_name);

  // Returns all the values in a certain column specified by colIdx
  const std::vector<uint64_t> getColVals(const int colIdx) const;

  // Returns the histogram with colVals passed in (result of calling getColVals)
  const std::vector<uint64_t> constructHistogram(const std::vector<uint64_t> colVals) const;

  const std::tuple<std::vector<std::map<uint64_t, std::vector<uint64_t>>>, std::vector<std::vector<std::vector<uint64_t>>>>
  makeHashTables() const;

  void modifyHashStuff(std::tuple<std::vector<std::map<uint64_t, std::vector<uint64_t>>>, std::vector<std::vector<std::vector<uint64_t>>>> args) const;
};
