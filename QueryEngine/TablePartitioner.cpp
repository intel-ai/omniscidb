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

#include "TablePartitioner.h"

#include "ColumnFetcher.h"

#include "ThriftSerializers.h"

TablePartitioner::TablePartitioner(const RelAlgExecutionUnit& ra_exe_unit,
                                   std::vector<InputColDescriptor> key_cols,
                                   std::vector<InputColDescriptor> payload_cols,
                                   const InputTableInfo& info,
                                   ColumnCacheMap& column_cache,
                                   PartitioningOptions po,
                                   Executor* executor,
                                   std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner)
    : key_cols_(std::move(key_cols))
    , payload_cols_(std::move(payload_cols))
    , info_(info)
    , column_cache_(column_cache)
    , po_(std::move(po))
    , executor_(executor)
    , ra_exe_unit_(ra_exe_unit)
    , row_set_mem_owner_(row_set_mem_owner) {
  auto pcnt = getPartitionsCount();
  histograms_.resize(info_.info.fragments.size(), std::vector<size_t>(pcnt, 0));
}

void TablePartitioner::fetchFragment(const Fragmenter_Namespace::FragmentInfo& frag,
                                     size_t frag_num,
                                     std::vector<const Analyzer::ColumnVar*>& vars,
                                     std::vector<const int8_t*>& output) {
  const int8_t* col_frag = nullptr;
  size_t elem_count = 0;
  for (auto var : vars) {
    std::tie(col_frag, elem_count) =
        ColumnFetcher::getOneColumnFragment(executor_,
                                            *(var),
                                            frag,
                                            Data_Namespace::CPU_LEVEL,
                                            0,
                                            chunks_owner_,
                                            column_cache_);
    if (col_frag == nullptr) {
      continue;
    }
    CHECK_NE(elem_count, size_t(0));
    output.push_back(col_frag);
  }
}

void TablePartitioner::fetchFragments(
    std::vector<const Analyzer::ColumnVar*>& key_vars,
    std::vector<const Analyzer::ColumnVar*>& payload_vars) {
  // get all fragments for columns separately
  // so far only one key column is considered
  std::vector<const int8_t*> empty;
  key_data_.assign(info_.info.fragments.size(), empty);
  payload_data_.assign(info_.info.fragments.size(), empty);
  size_t frag_idx = 0;
  for (auto& frag : info_.info.fragments) {
    fetchFragment(frag, frag_idx, key_vars, key_data_[frag_idx]);
    fetchFragment(frag, frag_idx, payload_vars, payload_data_[frag_idx]);
    frag_idx++;
  }
}

TemporaryTable TablePartitioner::runPartitioning() {
  std::vector<const Analyzer::ColumnVar*> key_vars;
  std::vector<const Analyzer::ColumnVar*> payload_vars;
  // Prepare some aux structures for memory descriptors and result sets.
  // ColSlotContext consumes Expr plain pointers and we use col_vars
  // to own memory. It can be released after ColSlotContext creation.
  std::vector<std::shared_ptr<Analyzer::ColumnVar>> col_vars;
  std::vector<Analyzer::Expr*> slots;
  std::vector<TargetInfo> targets;
  for (auto& col : key_cols_) {
    auto col_var = createColVar(col);
    slots.push_back(col_var.get());
    // fill info for obtaining key columns
    key_vars.push_back(col_var.get());
    key_sizes_.push_back(col_var->get_type_info().get_size());
    targets.emplace_back(
        TargetInfo{false, kMIN, col_var->get_type_info(), SQLTypeInfo(), false, false});
    col_vars.emplace_back(std::move(col_var));
  }
  for (auto& col : payload_cols_) {
    auto col_var = createColVar(col);
    slots.push_back(col_var.get());
    // fill info for obtaining payload columns
    payload_vars.push_back(col_var.get());
    payload_sizes_.push_back(col_var->get_type_info().get_size());
    targets.emplace_back(
        TargetInfo{false, kMIN, col_var->get_type_info(), SQLTypeInfo(), false, false});
    col_vars.emplace_back(std::move(col_var));
  }

  // fetch column data - keys and payloads
  fetchFragments(key_vars, payload_vars);

  // This vector holds write positions (in number of elements, not bytes)
  // in partitions for each partitioned fragment.
  // [fragment idx][partition id] -> offset in partition buffer.
  std::vector<std::vector<size_t>> partition_offsets;
  computePartitionSizesAndOffsets(partition_offsets);

  std::vector<ssize_t> dummy;
  ColSlotContext slot_ctx(slots, dummy);
  slot_ctx.setAllSlotsPaddedSizeToLogicalSize();
  std::vector<ResultSetPtr> partitions;
  // [partition id][column idx] -> column buffer.
  std::vector<std::vector<int8_t*>> col_bufs;
  col_bufs.resize(partition_sizes_.size());
  for (size_t frag_id = 0; frag_id < partition_sizes_.size(); ++frag_id) {
    if (!partition_sizes_[frag_id]) {
      partitions.push_back(nullptr);
      continue;
    }

    QueryMemoryDescriptor mem_desc(QueryDescriptionType::Projection,
                                   executor_,
                                   slot_ctx,
                                   partition_sizes_[frag_id],
                                   true);
    auto rs = std::make_shared<ResultSet>(
        targets, ExecutorDeviceType::CPU, mem_desc, row_set_mem_owner_, executor_);
    rs->setCachedRowCount(partition_sizes_[frag_id]);
    col_bufs[frag_id].resize(targets.size(), nullptr);

    // Init storage for non-empty partitions only.
    if (partition_sizes_[frag_id]) {
      auto* storage = rs->allocateStorage();
      for (size_t col_idx = 0; col_idx < targets.size(); ++col_idx) {
        size_t col_offs = mem_desc.getColOffInBytes(col_idx);
        int8_t* col_buf = storage->getUnderlyingBuffer() + col_offs;
        col_bufs[frag_id][col_idx] = col_buf;
      }
    }
    partitions.push_back(rs);
  }

  for (size_t i = 0; i < info_.info.fragments.size(); ++i) {
    // run partitioning function
    doPartition(i, partition_offsets, col_bufs);
  }

// TODO: remove debug prints
#if PARTITIONING_DEBUG_PRINT
  for (size_t pid = 0; pid < partitions.size(); ++pid) {
    std::cerr << "========== PARTITION " << pid << " ==========" << std::endl;
    for (size_t rid = 0; rid < partitions[pid]->entryCount(); ++rid) {
      auto row = partitions[pid]->getRowAt(rid);
      for (auto& val : row) {
        auto scalar_r = boost::get<ScalarTargetValue>(&val);
        CHECK(scalar_r);
        std::cerr << *scalar_r << " ";
      }
      std::cerr << std::endl;
    }
    std::cerr << "=================================" << std::endl;
  }
#endif

  return TemporaryTable(partitions, true);
}

size_t TablePartitioner::getPartitionsCount() const {
  return 1 << po_.mask_bits;
}

#define HASH_BIT_MODULO(K, MASK, NBITS) (((K)&MASK) >> NBITS)

uint32_t TablePartitioner::getHashValue(const int8_t* key,
                                        int size,
                                        int mask,
                                        int shift) {
  uint64_t value = 0;
  // FIXME: only one key column!
  switch (size) {
    case 2:
      value = *((int16_t*)key);
      break;
    case 4:
      value = *((int32_t*)key);
      break;
    case 8:
      value = *((int64_t*)key);
      break;
    default:
      CHECK(false);
  }

  return HASH_BIT_MODULO(value, mask, shift);
}

void TablePartitioner::collectHistogram(int frag_idx, std::vector<size_t>& histogram) {
  uint64_t i;
  const uint32_t fanOut = getPartitionsCount();
  // FIXME: only one key column for now!
  auto fragment = key_data_[frag_idx].at(0);
  auto fragment_size = info_.info.fragments[frag_idx].getNumTuples();
  // FIXME: only one key column for now!
  auto key_size = key_sizes_.at(0);
  for (i = 0; i < fragment_size; ++i) {
    uint32_t idx =
        getHashValue(&(fragment[i * key_size]), key_size, fanOut - 1, po_.scale_bits);
    histogram[idx]++;
  }

  /* compute local prefix sum on hist */
#if 0
  uint64_t sum = 0;
  for (i = 0; i < fanOut; i++) {
    sum += histogram[i];
    histogram[i] = sum;
  }
#endif
}

void TablePartitioner::computePartitionSizesAndOffsets(
    std::vector<std::vector<size_t>>& partition_offsets) {
  auto pcnt = getPartitionsCount();

  // Run histogram collection.
  for (size_t i = 0; i < info_.info.fragments.size(); ++i) {
    // run histogram collection function.
    collectHistogram(i, histograms_[i]);
  }

  // Count partition sizes and offsets (in number of tuples).
  partition_offsets.assign(info_.info.fragments.size(), std::vector<size_t>(pcnt, 0));
  for (size_t i = 0; i < (histograms_.size() - 1); ++i) {
    for (size_t j = 0; j < pcnt; ++j) {
      partition_offsets[i + 1][j] = partition_offsets[i][j] + histograms_[i][j];
    }
  }

  partition_sizes_.resize(pcnt, 0);
  for (size_t j = 0; j < pcnt; ++j) {
    partition_sizes_[j] = partition_offsets.back()[j] + histograms_.back()[j];
  }
}

#define CACHE_LINE_SIZE 64

void TablePartitioner::nonTempStore(int8_t* dst, const int8_t* src, size_t size) {
  if (size != 64) {
    memcpy(dst, src, size);
    return;
  }

#ifdef __AVX__
  register __m256i* d1 = (__m256i*)dst;
  register __m256i s1 = *((__m256i*)src);
  register __m256i* d2 = d1 + 1;
  register __m256i s2 = *(((__m256i*)src) + 1);

  _mm256_stream_si256(d1, s1);
  _mm256_stream_si256(d2, s2);

#elif defined(__SSE2__)

  register __m128i* d1 = (__m128i*)dst;
  register __m128i* d2 = d1 + 1;
  register __m128i* d3 = d1 + 2;
  register __m128i* d4 = d1 + 3;
  register __m128i s1 = *(__m128i*)src;
  register __m128i s2 = *((__m128i*)src + 1);
  register __m128i s3 = *((__m128i*)src + 2);
  register __m128i s4 = *((__m128i*)src + 3);

  _mm_stream_si128(d1, s1);
  _mm_stream_si128(d2, s2);
  _mm_stream_si128(d3, s3);
  _mm_stream_si128(d4, s4);

#else
  // Regular memcpy
  memcpy(dst, src, size);
#endif
}

// If enabled, use software-write-combined-buffer technique to fill partitions
void TablePartitioner::copyWithSWCB(int8_t* swcb_buf,
                                    size_t swcb_buf_size,
                                    int8_t* real_dst,
                                    const int8_t* src,
                                    size_t elem_size) {
  auto swcb_limit = CACHE_LINE_SIZE / elem_size - 1;
  auto pos = swcb_buf_size & swcb_limit;
  memcpy(swcb_buf + pos * elem_size, src, elem_size);
  // drop buffer in a partition when it is full
  if (swcb_buf_size == swcb_limit) {
    nonTempStore(real_dst - swcb_limit * elem_size, swcb_buf, CACHE_LINE_SIZE);
  }
}

void TablePartitioner::remainderCopyWithSWCB(int8_t* swcb_buf,
                                             size_t swcb_buf_size,
                                             int8_t* real_dst,
                                             size_t elem_size) {
  auto swcb_limit = CACHE_LINE_SIZE / elem_size - 1;
  auto rem_size = swcb_buf_size & swcb_limit;
  real_dst -= rem_size * elem_size;
  for (uint32_t j = 0; j < rem_size; ++j) {
    memcpy(real_dst, swcb_buf, elem_size);
    real_dst += elem_size;
    swcb_buf += elem_size;
  }
}

void TablePartitioner::initSWCBuffers(int frag_idx,
                                      const uint32_t fanOut,
                                      std::vector<std::vector<int8_t*>>& swcb_bufs,
                                      std::vector<size_t>& swcb_sizes) {
  // Fill initial information about swcb buffers and allocate them
  swcb_sizes.resize(fanOut, 0);
  swcb_bufs.resize(fanOut);
  for (uint32_t i = 0; i < fanOut; ++i) {
    // swcb_sizes[i] = partition_offsets[frag_idx][i];
    for (uint32_t j = 0; j < key_sizes_.size(); j++) {
      if (histograms_[frag_idx][i] > 0) {
        auto alloc_buf = (int8_t*)aligned_alloc(
            CACHE_LINE_SIZE, key_sizes_[j] * (CACHE_LINE_SIZE / key_sizes_[j]));
        if (!alloc_buf)
            throw std::runtime_error("Not enough memory for software-combined buffer");;
        swcb_bufs[i].push_back(alloc_buf);
      }
    }
    for (uint32_t j = 0; j < payload_sizes_.size(); ++j) {
      if (histograms_[frag_idx][i] > 0) {
        auto alloc_buf = (int8_t*)aligned_alloc(
            CACHE_LINE_SIZE, payload_sizes_[j] * (CACHE_LINE_SIZE / payload_sizes_[j]));
        if (!alloc_buf)
            throw std::runtime_error("Not enough memory for software-combined buffer");
        swcb_bufs[i].push_back(alloc_buf);
      }
    }
  }
}

void TablePartitioner::finalizeSWCBuffers(
    int frag_idx,
    const uint32_t fanOut,
    std::vector<std::vector<size_t>>& partition_offsets,
    std::vector<std::vector<int8_t*>>& col_bufs,
    std::vector<std::vector<int8_t*>>& swcb_bufs,
    std::vector<size_t>& swcb_sizes) {
  for (uint32_t idx = 0; idx < fanOut; ++idx) {
    // FIXME: only one key column for now!
    auto key_size = key_sizes_.at(0);
    auto off = partition_offsets[frag_idx][idx];
    int size = swcb_sizes[idx];
    if (size > 0) {
      remainderCopyWithSWCB(&(swcb_bufs[idx][0][0]),
                            size,
                            &(col_bufs[idx][0][(off + size) * key_size]),
                            key_size);
      if (payload_cols_.size() > 0) {
        int payload_idx = key_cols_.size();
        for (uint32_t payload_num = 0; payload_num < payload_data_[frag_idx].size();
             ++payload_num) {
          auto payload_size = payload_sizes_.at(payload_num);
          remainderCopyWithSWCB(&(swcb_bufs[idx][payload_idx][0]),
                                size,
                                &(col_bufs[idx][payload_idx][(off + size) * key_size]),
                                payload_size);
          payload_idx++;
        }
      }
    }
  }
  for (uint32_t i = 0; i < fanOut; ++i) {
    for (uint32_t j = 0; j < swcb_bufs[i].size(); j++) {
      free(swcb_bufs[i][j]);
    }
  }
}

void TablePartitioner::doPartition(int frag_idx,
                                   std::vector<std::vector<size_t>>& partition_offsets,
                                   std::vector<std::vector<int8_t*>>& col_bufs) {
  const uint32_t fanOut = getPartitionsCount();
  // Software combine buffers
  std::vector<std::vector<int8_t*>> swcb_bufs;
  // this will held info about number of elements in key's or payload's swcb buffers
  std::vector<size_t> swcb_sizes;
  if (g_radix_use_swcb) {
    initSWCBuffers(frag_idx, fanOut, swcb_bufs, swcb_sizes);
  }
  // FIXME: only one key column for now!
  auto key_size = key_sizes_.at(0);
  auto keys = key_data_.at(frag_idx).at(0);
  auto fragment_size = info_.info.fragments.at(frag_idx).getNumTuples();
  for (uint64_t i = 0; i < fragment_size; ++i) {
    uint32_t idx =
        getHashValue(&(keys[i * key_size]), key_size, fanOut - 1, po_.scale_bits);
    // fill partition - first key, then payload(s) if needed
    auto curr_off = partition_offsets[frag_idx][idx];
    // FIXME: only one key column for now!
    if (g_radix_use_swcb) {
      auto size = swcb_sizes[idx];
      copyWithSWCB(&(swcb_bufs[idx][0][0]),
                   size,
                   &(col_bufs[idx][0][(size + curr_off) * key_size]),
                   &(keys[i * key_size]),
                   key_size);
    } else
      memcpy(&(col_bufs[idx][0][curr_off * key_size]), &(keys[i * key_size]), key_size);
    if (payload_cols_.size() > 0) {
      int payload_num = 0;
      for (auto payload : payload_data_[frag_idx]) {
        auto payload_size = payload_sizes_.at(payload_num);
        auto payload_idx = payload_num + key_cols_.size();
        if (g_radix_use_swcb) {
          auto size = swcb_sizes[idx];
          copyWithSWCB(&(swcb_bufs[idx][payload_idx][0]),
                       size,
                       &(col_bufs[idx][payload_idx][(size + curr_off) * payload_size]),
                       &(payload[i * payload_size]),
                       payload_size);
        } else
          memcpy(&(col_bufs[idx][payload_idx][curr_off * payload_size]),
                 &(payload[i * payload_size]),
                 payload_size);
        payload_num++;
      }
    }
    if (!g_radix_use_swcb)
      partition_offsets[frag_idx][idx]++;
    else
      swcb_sizes[idx]++;
  }
  // Write remainder in case of SWCB usage
  if (g_radix_use_swcb) {
    finalizeSWCBuffers(
        frag_idx, fanOut, partition_offsets, col_bufs, swcb_bufs, swcb_sizes);
  }
}

std::shared_ptr<Analyzer::ColumnVar> TablePartitioner::createColVar(
    const InputColDescriptor& col) {
  auto& cat = *executor_->getCatalog();
  auto table_id = col.getScanDesc().getTableId();

  if (table_id < 0)
    throw std::runtime_error(
        "Failed to patition table: virtual tables are not supported");

  auto* desc = get_column_descriptor(col.getColId(), table_id, cat);
  CHECK(desc);

  return std::make_shared<Analyzer::ColumnVar>(
      desc->columnType, table_id, col.getColId(), col.getScanDesc().getNestLevel());
}
