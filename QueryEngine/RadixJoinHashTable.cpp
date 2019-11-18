/*
 * Copyright 2019 MapD Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "RadixJoinHashTable.h"
#include "Execute.h"
#include "JoinHashTable.h"

namespace {

void dumpRange(int first, int last, const std::string& prefix) {
  std::cerr << prefix << first;
  if (first != last)
    std::cerr << "-" << last;
}

}  // namespace

std::shared_ptr<RadixJoinHashTable> RadixJoinHashTable::getInstance(
    const std::shared_ptr<Analyzer::BinOper> qual_bin_oper,
    const std::vector<InputTableInfo>& query_infos,
    const Data_Namespace::MemoryLevel memory_level,
    const HashType preferred_hash_type,
    const int device_count,
    ColumnCacheMap& column_cache,
    Executor* executor) {
  auto join_hash_table =
      std::shared_ptr<RadixJoinHashTable>(new RadixJoinHashTable(qual_bin_oper,
                                                                 query_infos,
                                                                 memory_level,
                                                                 preferred_hash_type,
                                                                 device_count,
                                                                 column_cache,
                                                                 executor));
  join_hash_table->reify(device_count);

  return join_hash_table;
}

RadixJoinHashTable::RadixJoinHashTable(
    const std::shared_ptr<Analyzer::BinOper> qual_bin_oper,
    const std::vector<InputTableInfo>& query_infos,
    const Data_Namespace::MemoryLevel memory_level,
    const HashType preferred_hash_type,
    const int device_count,
    ColumnCacheMap& column_cache,
    Executor* executor)
    : qual_bin_oper_(qual_bin_oper)
    , query_infos_(query_infos)
    , memory_level_(memory_level)
    , layout_(HashType::OneToMany /*preferred_hash_type*/)
    , column_cache_(column_cache)
    , executor_(executor) {
  inner_outer_pairs_ = normalize_column_pairs(
      qual_bin_oper.get(), *(executor->getCatalog()), executor->getTemporaryTables());
  CHECK(!inner_outer_pairs_.empty());
  const auto& query_info = get_inner_query_info(getInnerTableId(), query_infos).info;

  for (auto frag : query_info.fragments) {
    Fragmenter_Namespace::TableInfo part_info;
    part_info.chunkKeyPrefix = query_info.chunkKeyPrefix;
    part_info.fragments.emplace_back(frag);

    // Baseline hash doesn't copy input table infos and holds
    // a reference instead. So keep them in part_query_infos_.
    part_query_infos_.emplace_back();
    auto& part_infos = part_query_infos_.back();
    part_infos.emplace_back(InputTableInfo{getInnerTableId(), part_info});

    part_tables_.emplace(
        std::make_pair(frag.fragmentId,
                       BaselineJoinHashTable::getInstance(
                           qual_bin_oper,
                           part_infos,
                           memory_level,
                           // Currently all base hash tables share the same layout
                           HashType::OneToMany /*preferred_hash_type*/,
                           device_count,
                           column_cache,
                           executor,
                           true)));
  }
}

size_t RadixJoinHashTable::shardCount() const {
  if (memory_level_ != Data_Namespace::GPU_LEVEL) {
    return 0;
  }
  return BaselineJoinHashTable::getShardCountForCondition(
      qual_bin_oper_.get(), executor_, inner_outer_pairs_);
}

int64_t RadixJoinHashTable::getJoinHashBuffer(const ExecutorDeviceType device_type,
                                              const int device_id,
                                              const int partition_id) const noexcept {
  CHECK(false)
      << "RadixJoinHashTable doesn't support plain buffers. Use descriptors instead.";
  return 0;
}

size_t RadixJoinHashTable::getJoinHashBufferSize(const ExecutorDeviceType device_type,
                                                 const int device_id,
                                                 const int partition_id) const noexcept {
  CHECK(false)
      << "RadixJoinHashTable doesn't support plain buffers. Use descriptors instead.";
  return 0;
}

int64_t RadixJoinHashTable::getJoinHashDescriptorPtr(const ExecutorDeviceType device_type,
                                                     const int device_id,
                                                     const int partition_id) const
    noexcept {
  auto it = part_tables_.find(partition_id);
  CHECK(it != part_tables_.end())
      << "An attempt to get hash table descriptor for missing part";
  return it->second->getJoinHashDescriptorPtr(device_type, device_id);
}

std::string RadixJoinHashTable::toString(const ExecutorDeviceType device_type,
                                         const int device_id,
                                         bool raw) const noexcept {
  std::stringstream ss;
  for (auto& pr : part_tables_) {
    ss << "P" << pr.first << " { " << pr.second->toString(device_type, device_id, raw)
       << " } ";
  }
  return ss.str();
}

std::set<DecodedJoinHashBufferEntry> RadixJoinHashTable::decodeJoinHashBuffer(
    const ExecutorDeviceType device_type,
    const int device_id) const noexcept {
  std::set<DecodedJoinHashBufferEntry> res;
  for (auto& pr : part_tables_) {
    auto part_res = pr.second->decodeJoinHashBuffer(device_type, device_id);
    res.insert(part_res.begin(), part_res.end());
  }
  return res;
}

llvm::Value* RadixJoinHashTable::codegenSlot(const CompilationOptions& co,
                                             const size_t index) {
  CHECK(false) << "RadixJoinHashTable doesn't support OneToOne layout";
  return nullptr;
}

HashJoinMatchingSet RadixJoinHashTable::codegenMatchingSet(const CompilationOptions& co,
                                                           const size_t index) {
  // All tables are of the same type and use size agnostic codegen, so pick any and use it
  // to generate code.
  CHECK(!part_tables_.empty());
  return part_tables_.begin()->second->codegenMatchingSet(co, index);
}

int RadixJoinHashTable::getInnerTableId() const noexcept {
  CHECK(!inner_outer_pairs_.empty());
  const auto first_inner_col = inner_outer_pairs_.front().first;
  return first_inner_col->get_table_id();
}

int RadixJoinHashTable::getInnerTableRteIdx() const noexcept {
  CHECK(!inner_outer_pairs_.empty());
  const auto first_inner_col = inner_outer_pairs_.front().first;
  return first_inner_col->get_rte_idx();
}

JoinHashTableInterface::HashType RadixJoinHashTable::getHashType() const noexcept {
  return layout_;
}

size_t RadixJoinHashTable::offsetBufferOff(const int partition_id) const noexcept {
  auto it = part_tables_.find(partition_id);
  if (it != part_tables_.end())
    return it->second->offsetBufferOff();
  return 0;
}

size_t RadixJoinHashTable::countBufferOff(const int partition_id) const noexcept {
  auto it = part_tables_.find(partition_id);
  if (it != part_tables_.end())
    return it->second->countBufferOff();
  return 0;
}

size_t RadixJoinHashTable::payloadBufferOff(const int partition_id) const noexcept {
  auto it = part_tables_.find(partition_id);
  if (it != part_tables_.end())
    return it->second->payloadBufferOff();
  return 0;
}

void RadixJoinHashTable::reify(const int device_count) {
  for (auto pr : part_tables_)
    dynamic_cast<BaselineJoinHashTable*>(pr.second.get())->reify(device_count);
}

size_t RadixJoinHashTable::dump(size_t entry_limit) const {
  std::set<int> ids;
  for (auto& pr : part_tables_)
    ids.insert(pr.first);

  size_t res = 0;
  for (auto it = ids.begin(); it != ids.end(); ++it) {
    if (res >= entry_limit) {
      std::cerr << "Partitions out of rows limit:";
      int first = *(it++);
      int last = first;
      while (it != ids.end()) {
        if (*it == last + 1)
          ++last;
        else {
          dumpRange(first, last, " ");
          first = *it;
          last = first;
        }
      }
      dumpRange(first, last, " ");
      std::cerr << std::endl;
      break;
    }

    res += dumpPartition(*it, entry_limit - res);
  }

  return res;
}

size_t RadixJoinHashTable::dumpPartition(size_t partition_id, size_t entry_limit) const {
  auto it = part_tables_.find(partition_id);
  if (it == part_tables_.end()) {
    std::cerr << "Partition #" << partition_id << " doesn't exist" << std::endl;
    return 0;
  }

  std::cerr << "========== Dump for radix join table part #" << partition_id
            << " ==========" << std::endl;
  auto table = part_tables_.at(partition_id);
  return table->dump(entry_limit);
}