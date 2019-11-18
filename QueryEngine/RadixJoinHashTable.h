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

#ifndef QUERYENGINE_RADIXJOINHASHTABLE_H
#define QUERYENGINE_RADIXJOINHASHTABLE_H

#include "../Analyzer/Analyzer.h"
#include "../Catalog/Catalog.h"
#include "../Chunk/Chunk.h"
#include "../Shared/ExperimentalTypeUtilities.h"
#include "Allocators/ThrustAllocator.h"
#include "BaselineJoinHashTable.h"
#include "ColumnarResults.h"
#include "Descriptors/InputDescriptors.h"
#include "Descriptors/RowSetMemoryOwner.h"
#include "ExpressionRange.h"
#include "InputMetadata.h"
#include "JoinHashTableInterface.h"

#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>

class RadixJoinHashTable : public JoinHashTableInterface {
 public:
  //! Make hash table from an in-flight SQL query's parse tree etc.
  static std::shared_ptr<RadixJoinHashTable> getInstance(
      const std::shared_ptr<Analyzer::BinOper> qual_bin_oper,
      const std::vector<InputTableInfo>& query_infos,
      const Data_Namespace::MemoryLevel memory_level,
      const HashType preferred_hash_type,
      const int device_count,
      ColumnCacheMap& column_cache,
      Executor* executor);

  //! Make hash table from named tables and columns (such as for testing).
  static std::shared_ptr<RadixJoinHashTable> getSyntheticInstance(
      std::string_view table1,
      std::string_view column1,
      std::string_view table2,
      std::string_view column2,
      const Data_Namespace::MemoryLevel memory_level,
      const HashType preferred_hash_type,
      const int device_count,
      ColumnCacheMap& column_cache,
      Executor* executor);

  size_t shardCount() const;

  int64_t getJoinHashBuffer(const ExecutorDeviceType device_type,
                            const int device_id,
                            const int partition_id) const noexcept override;

  size_t getJoinHashBufferSize(const ExecutorDeviceType device_type,
                               const int device_id,
                               const int partition_id) const noexcept override;

  int64_t getJoinHashDescriptorPtr(const ExecutorDeviceType device_type,
                                   const int device_id,
                                   const int partition_id = -1) const noexcept override;

  bool isPartitioned() const noexcept override { return true; }

  bool useDescriptors() const noexcept override { return true; }

  std::string toString(const ExecutorDeviceType device_type,
                       const int device_id,
                       bool raw = false) const noexcept override;

  std::set<DecodedJoinHashBufferEntry> decodeJoinHashBuffer(
      const ExecutorDeviceType device_type,
      const int device_id) const noexcept override;

  llvm::Value* codegenSlot(const CompilationOptions&,
                           const size_t) override;

  HashJoinMatchingSet codegenMatchingSet(const CompilationOptions&,
                                         const size_t) override;

  int getInnerTableId() const noexcept override;

  int getInnerTableRteIdx() const noexcept override;

  JoinHashTableInterface::HashType getHashType() const noexcept override;

  size_t offsetBufferOff(const int partition_id) const noexcept override;

  size_t countBufferOff(const int partition_id) const noexcept override;

  size_t payloadBufferOff(const int partition_id) const noexcept override;

  virtual ~RadixJoinHashTable() {}

  size_t dump(size_t entry_limit = 200) const override;
  size_t dumpPartition(size_t partition_id, size_t entry_limit = 200) const;

 private:
  RadixJoinHashTable(const std::shared_ptr<Analyzer::BinOper> qual_bin_oper,
                     const std::vector<InputTableInfo>& query_infos,
                     const Data_Namespace::MemoryLevel memory_level,
                     const HashType preferred_hash_type,
                     const int device_count,
                     ColumnCacheMap& column_cache,
                     Executor* executor);

  void reify(const int device_count);

  const std::shared_ptr<Analyzer::BinOper> qual_bin_oper_;
  const std::vector<InputTableInfo>& query_infos_;
  const Data_Namespace::MemoryLevel memory_level_;
  HashType layout_;
  ColumnCacheMap& column_cache_;
  Executor* executor_;
  std::vector<InnerOuter> inner_outer_pairs_;
  std::unordered_map<int, std::shared_ptr<JoinHashTableInterface>> part_tables_;
  std::deque<std::vector<InputTableInfo>> part_query_infos_;
};

#endif  // QUERYENGINE_RADIXJOINHASHTABLE_H