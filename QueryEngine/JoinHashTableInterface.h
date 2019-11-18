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
#ifndef QUERYENGINE_JOINHASHTABLEINTERFACE_H
#define QUERYENGINE_JOINHASHTABLEINTERFACE_H

#include <llvm/IR/Value.h>
#include <cstdint>
#include <set>
#include <string>
#include "Analyzer/Analyzer.h"
#include "CompilationOptions.h"

class TooManyHashEntries : public std::runtime_error {
 public:
  TooManyHashEntries()
      : std::runtime_error("Hash tables with more than 2B entries not supported yet") {}
};

class TableMustBeReplicated : public std::runtime_error {
 public:
  TableMustBeReplicated(const std::string& table_name)
      : std::runtime_error("Hash join failed: Table '" + table_name +
                           "' must be replicated.") {}
};

class HashJoinFail : public std::runtime_error {
 public:
  HashJoinFail(const std::string& reason) : std::runtime_error(reason) {}
};

class FailedToFetchColumn : public HashJoinFail {
 public:
  FailedToFetchColumn()
      : HashJoinFail("Not enough memory for columns involvde in join") {}
};

class FailedToJoinOnVirtualColumn : public HashJoinFail {
 public:
  FailedToJoinOnVirtualColumn() : HashJoinFail("Cannot join on rowid") {}
};

struct HashJoinMatchingSet {
  llvm::Value* elements;
  llvm::Value* count;
  llvm::Value* slot;
};

struct DecodedJoinHashBufferEntry {
  std::vector<int64_t> key;
  std::set<int32_t> payload;

  bool operator<(const DecodedJoinHashBufferEntry& other) const {
    return std::tie(key, payload) < std::tie(other.key, other.payload);
  }

  bool operator==(const DecodedJoinHashBufferEntry& other) const {
    return key == other.key && payload == other.payload;
  }
};  // struct DecodedJoinHashBufferEntry

using InnerOuter = std::pair<const Analyzer::ColumnVar*, const Analyzer::Expr*>;

class JoinHashTableInterface {
 public:
  struct Descriptor {
    int8_t* buffer;
    int64_t entry_count;
  };

  virtual bool isPartitioned() const noexcept { return false; }

  // Return true if dynamic code is generated to used hash
  // table descriptors rather than plain buffer pointers.
  virtual bool useDescriptors() const noexcept { return false; }

  virtual int64_t getJoinHashBuffer(const ExecutorDeviceType device_type,
                                    const int device_id,
                                    const int partition_id = -1) const noexcept = 0;

  virtual size_t getJoinHashBufferSize(const ExecutorDeviceType device_type,
                                       const int device_id,
                                       const int partition_id = -1) const
      noexcept = 0;  // bytes

  virtual int64_t getJoinHashDescriptorPtr(const ExecutorDeviceType device_type,
                                           const int device_id,
                                           const int partition_id = -1) const
      noexcept = 0;

  virtual std::string toString(const ExecutorDeviceType device_type,
                               const int device_id,
                               bool raw = false) const noexcept = 0;

  virtual std::string toStringFlat64(const ExecutorDeviceType device_type,
                                     const int device_id) const noexcept;

  virtual std::string toStringFlat32(const ExecutorDeviceType device_type,
                                     const int device_id) const noexcept;

  virtual std::set<DecodedJoinHashBufferEntry> decodeJoinHashBuffer(
      const ExecutorDeviceType device_type,
      const int device_id) const noexcept = 0;

  virtual llvm::Value* codegenSlot(const CompilationOptions&, const size_t) = 0;

  virtual HashJoinMatchingSet codegenMatchingSet(const CompilationOptions&,
                                                 const size_t) = 0;

  virtual int getInnerTableId() const noexcept = 0;

  virtual int getInnerTableRteIdx() const noexcept = 0;

  enum class HashType { OneToOne, OneToMany };

  virtual HashType getHashType() const noexcept = 0;

  virtual size_t offsetBufferOff(const int partition_id = -1) const noexcept = 0;

  virtual size_t countBufferOff(const int partition_id = -1) const noexcept = 0;

  virtual size_t payloadBufferOff(const int partition_id = -1) const noexcept = 0;

  virtual size_t dump(size_t entry_limit = 200) const { return 0; }
};

std::string decodeJoinHashBufferToString(
    size_t key_component_count,  // number of key parts
    size_t key_component_width,  // width of a key part
    const int8_t* ptr1,          // hash entries
    const int8_t* ptr2,          // offsets
    const int8_t* ptr3,          // counts
    const int8_t* ptr4,          // payloads (rowids)
    size_t buffer_size,
    bool raw = false);

std::ostream& operator<<(std::ostream& os, const DecodedJoinHashBufferEntry& e);

std::ostream& operator<<(std::ostream& os, const std::set<DecodedJoinHashBufferEntry>& s);

std::set<DecodedJoinHashBufferEntry> decodeJoinHashBuffer(
    size_t key_component_count,  // number of key parts
    size_t key_component_width,  // width of a key part
    const int8_t* ptr1,          // hash entries
    const int8_t* ptr2,          // offsets
    const int8_t* ptr3,          // counts
    const int8_t* ptr4,          // payloads (rowids)
    size_t buffer_size);

inline std::ostream& operator<<(std::ostream& os, JoinHashTableInterface::HashType type) {
  if (type == JoinHashTableInterface::HashType::OneToOne)
    os << "OneToOne";
  else if (type == JoinHashTableInterface::HashType::OneToMany)
    os << "OneToMany";
  else
    CHECK(false) << "Unknown join hash table type (" << (int)type << ")";
  return os;
}

#endif  // QUERYENGINE_JOINHASHTABLEINTERFACE_H
