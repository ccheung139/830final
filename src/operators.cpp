#include "operators.h"

#include <cassert>
#include <iostream>
#include <algorithm>
#include <numeric>
// Get materialized results
std::vector<uint64_t *> Operator::getResults()
{
  uint64_t size = tmp_results_.size();
  std::vector<uint64_t *> result_vector(size);
  for (int i = 0; i < size; ++i)
  {
    result_vector[i] = tmp_results_[i].data();
  }
  return result_vector;
}

// Require a column and add it to results
bool Scan::require(SelectInfo info)
{
  if (info.binding != relation_binding_)
    return false;
  assert(info.col_id < relation_.columns().size());
  result_columns_.push_back(relation_.columns()[info.col_id]);
  select_to_result_col_id_[info] = result_columns_.size() - 1;
  return true;
}

// Run
void Scan::run()
{
  // Nothing to do
  result_size_ = relation_.size();
}

// Get materialized results
std::vector<uint64_t *> Scan::getResults()
{
  return result_columns_;
}

// Require a column and add it to results
bool FilterScan::require(SelectInfo info)
{
  if (info.binding != relation_binding_)
    return false;
  assert(info.col_id < relation_.columns().size());
  if (select_to_result_col_id_.find(info) == select_to_result_col_id_.end())
  {
    // Add to results
    input_data_.push_back(relation_.columns()[info.col_id]);
    tmp_results_.emplace_back();
    unsigned colId = tmp_results_.size() - 1;
    select_to_result_col_id_[info] = colId;
  }
  return true;
}

// Copy to result
void FilterScan::copy2Result(uint64_t id)
{
  for (unsigned cId = 0; cId < input_data_.size(); ++cId)
    tmp_results_[cId].push_back(input_data_[cId][id]);
  ++result_size_;
}

// Apply filter
bool FilterScan::applyFilter(uint64_t i, FilterInfo &f)
{
  auto compare_col = relation_.columns()[f.filter_column.col_id];
  auto constant = f.constant;
  switch (f.comparison)
  {
  case FilterInfo::Comparison::Equal:
    return compare_col[i] == constant;
  case FilterInfo::Comparison::Greater:
    return compare_col[i] > constant;
  case FilterInfo::Comparison::Less:
    return compare_col[i] < constant;
  };
  return false;
}

void FilterScan::mergeIntingTmpResults(int col)
{
  for (int threadIdx = 0; threadIdx < inting_tmp_results_.size(); ++threadIdx)
    {
      auto data = inting_tmp_results_[threadIdx][col];
      tmp_results_[col].insert(tmp_results_[col].end(), data.begin(), data.end());
    }
}

void FilterScan::copy2ResultInting(uint64_t index, uint64_t id)
{
  for (unsigned cId = 0; cId < input_data_.size(); ++cId)
    inting_tmp_results_[index][cId].push_back(input_data_[cId][id]);
  // ++inting_result_sizes_[index];
}

void FilterScan::runTask(uint64_t lowerBound, uint64_t upperBound, int index)
{
  inting_tmp_results_[index].resize(tmp_results_.size());
  for (uint64_t i = lowerBound; i < upperBound; ++i)
  {
    bool pass = true;
    for (auto &f : filters_)
    {
      pass &= applyFilter(i, f);
      if (!pass)
        break;
    }
    if (pass)
      copy2ResultInting(index, i);
  }
}

// Run
void FilterScan::run()
{
  if (relation_.size() > 1000) {
    std::vector<std::thread> threads;
    inting_tmp_results_.resize(NUM_THREADS);

    uint64_t size = relation_.size() / NUM_THREADS;
    for (int i = 0; i < NUM_THREADS - 1; ++i) {
      threads.push_back(std::thread(&FilterScan::runTask, this, size * i, size * (i + 1), i));
    }
    runTask((NUM_THREADS - 1) * size, relation_.size(), NUM_THREADS - 1);
    for (auto& thread : threads) {
      thread.join();
    }

    threads.clear();
    for (int col = 0; col < tmp_results_.size() - 1; ++col) {
      threads.push_back(std::thread(&FilterScan::mergeIntingTmpResults, this, col));
    }
    mergeIntingTmpResults(tmp_results_.size() - 1);
    for (auto& thread : threads) {
      thread.join();
    }
  } else {
    for (uint64_t i = 0; i < relation_.size(); ++i)
    {
      bool pass = true;
      for (auto &f : filters_)
      {
        pass &= applyFilter(i, f);
        if (!pass)
          break;
      }
      if (pass)
        copy2Result(i);
    }
  }
  result_size_ = tmp_results_[0].size();
}

// Require a column and add it to results
bool Join::require(SelectInfo info)
{
  if (requested_columns_.count(info) == 0)
  {
    bool success = false;
    if (left_->require(info))
    {
      requested_columns_left_.emplace_back(info);
      success = true;
    }
    else if (right_->require(info))
    {
      success = true;
      requested_columns_right_.emplace_back(info);
    }
    if (!success)
      return false;

    tmp_results_.emplace_back();
    requested_columns_.emplace(info);
  }
  return true;
}

// Copy to result
void Join::copy2ResultInting(uint64_t left_id, uint64_t right_id, uint64_t index)
{
  // left_inting_tmp_results_[index].push_back(left_id);
  // right_inting_tmp_results_[index].push_back(right_id);

  unsigned rel_col_id = 0;
  for (unsigned cId = 0; cId < copy_left_data_.size(); ++cId)
  {
    inting_tmp_results_[index][rel_col_id++].push_back(copy_left_data_[cId][left_id]);
  }

  for (unsigned cId = 0; cId < copy_right_data_.size(); ++cId)
  {
    inting_tmp_results_[index][rel_col_id++].push_back(copy_right_data_[cId][right_id]);
  }
}

void Join::mergeIntingTmpResults(int index, uint64_t offset)
{
  // for (int threadIdx = 0; threadIdx < inting_tmp_results_.size(); ++threadIdx)
  //   {
  //     auto data = inting_tmp_results_[threadIdx][col];
  //     tmp_results_[col].insert(tmp_results_[col].end(), data.begin(), data.end());
  //   }
  for (int col = 0; col < tmp_results_.size(); ++col) {
    auto& data = inting_tmp_results_[index][col];
    std::vector<uint64_t>::iterator it = tmp_results_[col].begin();
    std::copy(data.begin(), data.end(), tmp_results_[col].begin() + offset);
  }
}

void SelfJoin::mergeIntingTmpResults(int col)
{
  for (int threadIdx = 0; threadIdx < NUM_THREADS; ++threadIdx)
    {
      auto data = inting_tmp_results_[threadIdx][col];
      tmp_results_[col].insert(tmp_results_[col].end(), data.begin(), data.end());
    }
}

// Copy to result
void Join::copy2Result(uint64_t left_id, uint64_t right_id)
{
  unsigned rel_col_id = 0;
  for (unsigned cId = 0; cId < copy_left_data_.size(); ++cId)
    tmp_results_[rel_col_id++].push_back(copy_left_data_[cId][left_id]);

  for (unsigned cId = 0; cId < copy_right_data_.size(); ++cId)
    tmp_results_[rel_col_id++].push_back(copy_right_data_[cId][right_id]);
  ++result_size_;
}

// Run
void Join::run()
{
  left_->require(p_info_.left);
  right_->require(p_info_.right);
  right_->run();
  left_->run();
  

  // Use smaller input_ for build
  if (left_->result_size() > right_->result_size())
  {
    std::swap(left_, right_);
    std::swap(p_info_.left, p_info_.right);
    std::swap(requested_columns_left_, requested_columns_right_);
  }
  auto left_input_data = left_->getResults();
  auto right_input_data = right_->getResults();

  // Resolve the input_ columns_
  unsigned res_col_id = 0;
  for (auto &info : requested_columns_left_)
  {
    copy_left_data_.push_back(left_input_data[left_->resolve(info)]);
    select_to_result_col_id_[info] = res_col_id++;
  }
  for (auto &info : requested_columns_right_)
  {
    copy_right_data_.push_back(right_input_data[right_->resolve(info)]);
    select_to_result_col_id_[info] = res_col_id++;
  }

  auto left_col_id = left_->resolve(p_info_.left);
  auto right_col_id = right_->resolve(p_info_.right);

  // Build phase
  std::vector<std::thread> threads;
  uint64_t limit = left_->result_size();
  uint64_t size = limit / (NUM_THREADS);
  auto left_key_column = left_input_data[left_col_id];
  hash_table_.reserve(left_->result_size() * 2);
  for (uint64_t i = 0; i != limit; ++i)
  {
    hash_table_.emplace(left_key_column[i], i);
  }
  // Probe phase
  auto right_key_column = right_input_data[right_col_id];
  int desiredNumThreads = std::max((int)(right_->result_size() / 10000), 1);
  NUM_THREADS = std::min(desiredNumThreads, NUM_THREADS);
  inting_tmp_results_.resize(NUM_THREADS);
  inting_result_sizes_.resize(NUM_THREADS);

  limit = right_->result_size();
  size = limit / (NUM_THREADS);
  if (NUM_THREADS != 1) {
    
    for (int j = 0; j < NUM_THREADS - 1; j++)
    {
      threads.push_back(std::thread(&Join::runTask, this, j * size, size * (j + 1), j, right_key_column));
    }
    runTask((NUM_THREADS - 1) * size, limit, NUM_THREADS - 1, right_key_column);
    for (auto &thread : threads)
    {
      thread.join();
    }

    threads.clear();
    // for (int col = 0; col < tmp_results_.size() - 1; ++col) {
    //   threads.push_back(std::thread(&Join::mergeIntingTmpResults, this, col));
    // }
    // mergeIntingTmpResults(tmp_results_.size() - 1);
    // for (auto& thread : threads) {
    //   thread.join();
    // }

    uint64_t totalSize = std::accumulate(inting_result_sizes_.begin(), inting_result_sizes_.end(), 0);
    for (int col = 0; col < tmp_results_.size(); ++col) {
      tmp_results_[col].resize(totalSize);
    }
    uint64_t runningSumSize = 0; 
    for (int j = 0; j < NUM_THREADS - 1; ++j) {
      threads.push_back(std::thread(&Join::mergeIntingTmpResults, this, j, runningSumSize));
      runningSumSize += inting_tmp_results_[j][0].size();
    }
    mergeIntingTmpResults(NUM_THREADS - 1, runningSumSize);
    for (auto& thread : threads) {
      thread.join();
    }

  } else {
    auto right_key_column = right_input_data[right_col_id];
    for (uint64_t i = 0, limit = i + right_->result_size(); i != limit; ++i) {
      auto rightKey = right_key_column[i];
      auto range = hash_table_.equal_range(rightKey);
      for (auto iter = range.first; iter != range.second; ++iter) {
        copy2Result(iter->second, i);
      }
    }
  }
  result_size_ = tmp_results_[0].size();
  // std::cerr << "desired " << result_size_ << std::endl;
}

void Join::runTask(uint64_t lowerBound, uint64_t upperBound, int index, uint64_t *right_key_column)
{
  inting_tmp_results_[index].resize(tmp_results_.size());
  std::vector<std::vector<uint64_t>> &localCopy = inting_tmp_results_[index];
  for (uint64_t i = lowerBound; i < upperBound; ++i)
  {
    auto rightKey = right_key_column[i];
    auto range = hash_table_.equal_range(rightKey);
    for (auto iter = range.first; iter != range.second; ++iter)
    {
      // copy2ResultInting(iter->second, i, index);
      int left_id = iter->second;
      unsigned rel_col_id = 0;
      for (unsigned cId = 0; cId < copy_left_data_.size(); ++cId)
      {
        localCopy[rel_col_id++].push_back(copy_left_data_[cId][left_id]);
      }

      for (unsigned cId = 0; cId < copy_right_data_.size(); ++cId)
      {
        localCopy[rel_col_id++].push_back(copy_right_data_[cId][i]);
      }
    }
  }
  inting_result_sizes_[index] = localCopy[0].size();
}

void SelfJoin::copy2ResultInting(uint64_t i, int threadIndex)
{
  unsigned rel_col_id = 0;

  for (unsigned cId = 0; cId < copy_data_.size(); ++cId)
  {
    inting_tmp_results_[threadIndex][rel_col_id++].push_back(copy_data_[cId][i]);
  }
}

void SelfJoin::runTask(uint64_t lowerBound, uint64_t upperBound, int index, uint64_t *left_key_column, uint64_t *right_key_column)
{
  inting_tmp_results_[index].resize(tmp_results_.size());
  for (uint64_t i = lowerBound; i < upperBound; ++i)
  {
    if (left_key_column[i] == right_key_column[i])
      copy2ResultInting(i, index);
  }
}

// Copy to result
void SelfJoin::copy2Result(uint64_t id)
{
  for (unsigned cId = 0; cId < copy_data_.size(); ++cId)
    tmp_results_[cId].push_back(copy_data_[cId][id]);
  ++result_size_;
}

// Require a column and add it to results
bool SelfJoin::require(SelectInfo info)
{
  if (required_IUs_.count(info))
    return true;
  if (input_->require(info))
  {
    tmp_results_.emplace_back();
    required_IUs_.emplace(info);
    return true;
  }
  return false;
}

// Run
void SelfJoin::run()
{
  input_->require(p_info_.left);
  input_->require(p_info_.right);
  input_->run();
  input_data_ = input_->getResults();

  for (auto &iu : required_IUs_)
  {
    auto id = input_->resolve(iu);
    copy_data_.emplace_back(input_data_[id]);
    select_to_result_col_id_.emplace(iu, copy_data_.size() - 1);
  }

  auto left_col_id = input_->resolve(p_info_.left);
  auto right_col_id = input_->resolve(p_info_.right);

  auto left_col = input_data_[left_col_id];
  auto right_col = input_data_[right_col_id];

  inting_tmp_results_.resize(NUM_THREADS);
  // inting_result_sizes_.resize(NUM_THREADS);

  std::vector<std::thread> threads;
  uint64_t limit = input_->result_size();
  int desiredNumThreads = std::max((int)(limit / 1000), 1);
  NUM_THREADS = std::min(desiredNumThreads, NUM_THREADS);
  uint64_t size = limit / (NUM_THREADS);
  if (NUM_THREADS != 1) {
    for (int j = 0; j < NUM_THREADS - 1; j++)
    {
      threads.push_back(std::thread(&SelfJoin::runTask, this, j * size, size * (j + 1), j, left_col, right_col));
    }
    runTask((NUM_THREADS - 1) * size, limit, NUM_THREADS - 1, left_col, right_col);
    for (auto &thread : threads)
    {
      thread.join();
    }

    threads.clear();
    for (int col = 0; col < tmp_results_.size() - 1; ++col) {
      threads.push_back(std::thread(&SelfJoin::mergeIntingTmpResults, this, col));
    }
    mergeIntingTmpResults(tmp_results_.size() - 1);
    for (auto& thread : threads) {
      thread.join();
    }
  } else {
    for (uint64_t i = 0; i < input_->result_size(); ++i) {
      if (left_col[i] == right_col[i])
        copy2Result(i);
    }
  }
  
  result_size_ = tmp_results_[0].size();
}

// Run
void Checksum::run()
{
  for (auto &sInfo : col_info_)
  {
    input_->require(sInfo);
  }
  input_->run();
  std::vector<uint64_t *> results = input_->getResults();

  // std::vector<std::thread> threads;
  // check_sums_.resize(col_info_.size());
  // for (int col = 0; col < col_info_.size(); ++col)
  // {
  //   // runTask(col);
  //   threads.push_back(std::thread(&Checksum::runTask, this, col));
  // }
  // for (auto &thread : threads)
  // {
  //   thread.join();
  // }

  for (auto &sInfo : col_info_)
  {
    auto col_id = input_->resolve(sInfo);
    auto result_col = results[col_id];
    uint64_t sum = 0;
    result_size_ = input_->result_size();
    for (auto iter = result_col, limit = iter + input_->result_size();
         iter != limit;
         ++iter)
      sum += *iter;
    check_sums_.push_back(sum);
  }
}

// void Checksum::runTask(int col) {
//   auto &sInfo = col_info_[col];
//   auto col_id = input_->resolve(sInfo);
//   auto result_col = results[col_id];
//   uint64_t sum = 0;
//   result_size_ = input_->result_size();
//   for (auto iter = result_col, limit = iter + input_->result_size();
//         iter != limit;
//         ++iter)
//     sum += *iter;
//   check_sums_[col] = sum;
//   // check_sums_.push_back(sum);
// }
