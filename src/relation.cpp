#include "relation.h"
#include "joiner.h"
#include "operators.h"
#include <map>
#include <algorithm>

#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>

int NUM_BUCKETS = 10;

// Stores a relation into a binary file
void Relation::storeRelation(const std::string &file_name)
{
  std::ofstream out_file;
  out_file.open(file_name, std::ios::out | std::ios::binary);
  out_file.write((char *)&size_, sizeof(size_));
  auto numColumns = columns_.size();
  out_file.write((char *)&numColumns, sizeof(size_t));
  for (auto c : columns_)
  {
    out_file.write((char *)c, size_ * sizeof(uint64_t));
  }
  out_file.close();
}

// Stores a relation into a file (csv), e.g., for loading/testing it with a DBMS
void Relation::storeRelationCSV(const std::string &file_name)
{
  std::ofstream out_file;
  out_file.open(file_name + ".tbl", std::ios::out);
  for (uint64_t i = 0; i < size_; ++i)
  {
    for (auto &c : columns_)
    {
      out_file << c[i] << '|';
    }
    out_file << "\n";
  }
}

// Dump SQL: Create and load table (PostgreSQL)
void Relation::dumpSQL(const std::string &file_name, unsigned relation_id)
{
  std::ofstream out_file;
  out_file.open(file_name + ".sql", std::ios::out);
  // Create table statement
  out_file << "CREATE TABLE r" << relation_id << " (";
  for (unsigned cId = 0; cId < columns_.size(); ++cId)
  {
    out_file << "c" << cId << " bigint"
             << (cId < columns_.size() - 1 ? "," : "");
  }
  out_file << ");\n";
  // Load from csv statement
  out_file << "copy r" << relation_id << " from 'r" << relation_id
           << ".tbl' delimiter '|';\n";
}

void Relation::loadRelation(const char *file_name)
{
  int fd = open(file_name, O_RDONLY);
  if (fd == -1)
  {
    std::cerr << "cannot open " << file_name << std::endl;
    throw;
  }

  // Obtain file size_
  struct stat sb
  {
  };
  if (fstat(fd, &sb) == -1)
    std::cerr << "fstat\n";

  auto length = sb.st_size;

  char *addr = static_cast<char *>(mmap(nullptr,
                                        length,
                                        PROT_READ,
                                        MAP_PRIVATE,
                                        fd,
                                        0u));
  if (addr == MAP_FAILED)
  {
    std::cerr << "cannot mmap " << file_name << " of length " << length
              << std::endl;
    throw;
  }

  if (length < 16)
  {
    std::cerr << "relation_ file " << file_name
              << " does not contain a valid header"
              << std::endl;
    throw;
  }

  this->size_ = *reinterpret_cast<uint64_t *>(addr);
  addr += sizeof(size_);
  auto numColumns = *reinterpret_cast<size_t *>(addr);
  addr += sizeof(size_t);

  for (unsigned i = 0; i < numColumns; ++i)
  {
    this->columns_.push_back(reinterpret_cast<uint64_t *>(addr));
    addr += size_ * sizeof(uint64_t);
  }

  std::vector<std::vector<uint64_t>> histogramsForRelation;

  // hashtable = makeHashTables()
  // Operators::store

  for (int c = 0; c < this->columns_.size(); c++)
  {
    std::vector<uint64_t> colVals = getColVals(c);
    std::vector<uint64_t> histogramForCol = constructHistogram(colVals);

    histogramsForRelation.push_back(histogramForCol);
  }

  Joiner::appendHistogram(histogramsForRelation);

  std::tuple<std::vector<std::map<uint64_t, std::vector<uint64_t>>>, std::vector<std::vector<std::vector<uint64_t>>>>
      hashTablesAndSortedVals = makeHashTables();

  hashStuff = hashTablesAndSortedVals;

  // Operator::appendHashTablesAndSortedVals(hashTablesAndSortedVals);

  // hashTableAndSortedVals_ = hashTablesAndSortedVals;

  // std::vector<int> firstColVals = getColVals(0);
  // std::vector<int> histogram = constructHistogram(firstColVals);

  // std::cout << "NEW TEST CASE " << histogram.size() << std::endl;

  // for (int i = 0; i < histogram.size(); i++)
  // {
  //   std::cout << "Frequency for bucket " << i << " : " << histogram[i] << std::endl;
  // }

  // https://www.postgresql.org/docs/current/row-estimation-examples.html may be useful for different histogram estimates
  // Section on MCV can be useful if some of the queries have WHERE's that are = constant val (i.e. tablename = 'bonobo')
  // IntHistogram.java in lab3 mentions avgSelectivity function as means for more efficient optimization (should look into this)

  // we touched this
  // for (int j = 0; j < this->size_; j++)
  // {

  //   // for (int i = 0; i < this->columns_.size(); i++)
  //   // {
  //   auto &c(this->columns_[i]);
  //   std::cout << c[j] << std::endl;
  //   // }
  // }
}

/**
 * 0: equals hashtables
 *    vector of hashtables - for each column
 * 1: greater than hashtables
 * 2: less than hashtables
 * 3: minimums
 *    vector of minimum values - for each column
 * 4: maximums
 */
std::tuple<std::vector<std::map<uint64_t, std::vector<uint64_t>>>, std::vector<std::vector<std::vector<uint64_t>>>>
Relation::makeHashTables()
{
  std::vector<std::map<uint64_t, std::vector<uint64_t>>> resultEquals;
  std::vector<std::vector<std::vector<uint64_t>>> sortedVals;

  for (int c = 0; c < this->columns_.size(); ++c)
  {
    std::vector<uint64_t> colVals = getColVals(c);
    std::map<uint64_t, std::vector<uint64_t>> hashTable;

    std::vector<uint64_t> sortedColVals(colVals);
    std::vector<std::vector<uint64_t>> sortedColValsVec;

    std::map<uint64_t, uint64_t> valToIndex;
    // for (int i = 0; i < colVals.size())
    std::sort(sortedColVals.begin(), sortedColVals.end());

    for (int i = 0; i < colVals.size(); ++i)
    {
      uint64_t colVal = colVals[i];
      if (hashTable.count(colVal))
      {
        auto f = hashTable.find(colVal);
        std::vector<uint64_t> indices = f->second;
        indices.push_back(i);
      }
      else
      {
        std::vector<uint64_t> indices;
        indices.push_back(i);
        hashTable.insert(std::pair<uint64_t, std::vector<uint64_t>>(colVal, indices));
      }

      valToIndex.insert(std::pair<uint64_t, uint64_t>(colVal, i));
    }

    resultEquals.push_back(hashTable);
    for (uint64_t &colVals : sortedColVals)
    {
      std::vector<uint64_t> tuplesToAdd;
      tuplesToAdd.push_back(colVals);
      tuplesToAdd.push_back(valToIndex.find(colVals)->second);
      sortedColValsVec.push_back(tuplesToAdd);
    }

    sortedVals.push_back(sortedColValsVec);
  }

  return std::make_tuple(resultEquals, sortedVals);
}

// sortedColVals
// 0, 5
// 1, 2
// 4, 10

// 0, []
// 1, [5]
// 4, [5, 2]

std::vector<uint64_t> Relation::getColVals(int colIdx)
{
  std::vector<uint64_t> colVals(this->size_, 0);
  auto &c(this->columns_[colIdx]);
  for (uint64_t j = 0; j < this->size_; j++)
  {
    colVals[j] = c[j];
  }

  return colVals;
}

std::vector<uint64_t> Relation::constructHistogram(std::vector<uint64_t> colVals)
{

  uint64_t minVal, maxVal = colVals[0];

  for (uint64_t colVal : colVals)
  {
    minVal = std::min(colVal, minVal);
    maxVal = std::max(colVal, maxVal);
  }

  uint64_t bucketWidth = (1 + maxVal - minVal) / NUM_BUCKETS;
  // bucketWidth = std::max(1, bucketWidth); // Incase width gets integer divisioned down to 0

  std::vector<uint64_t> histogram(NUM_BUCKETS, 0);

  // A bit of repeated work as before when getting min and maxes (may be better to combine into one)
  for (uint64_t &colVal : colVals)
  {
    uint64_t bucket = (colVal - minVal) / bucketWidth;
    if (bucket >= NUM_BUCKETS)
    {
      bucket = NUM_BUCKETS - 1; // If overflow, just fit it into last bucket
    }
    histogram[bucket]++;
  }

  // *minValPtr = minVal;
  // *maxValPtr = maxVal;
  // *bucketWidthPtr = bucketWidth;

  histogram.push_back(minVal);
  histogram.push_back(maxVal);
  histogram.push_back(bucketWidth);
  histogram.push_back(this->size_);

  return histogram;
}

// Constructor that loads relation_ from disk
Relation::Relation(const char *file_name) : owns_memory_(false), size_(0)
{
  loadRelation(file_name);
}

// Destructor
Relation::~Relation()
{
  if (owns_memory_)
  {
    for (auto c : columns_)
      delete[] c;
  }
}
