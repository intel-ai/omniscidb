/*
 * Copyright 2020 OmniSci, Inc.
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
#pragma once

#include <iostream>
#include <memory>
#include <vector>

// export L0_SAFE_CALL -- remove?
#include "L0Mgr/L0Exception.h"
#include "L0Mgr/Utils.h"

#include <level_zero/ze_api.h>

namespace l0 {

#ifdef HAVE_L0
class L0Device;
class L0Driver {
 private:
  ze_context_handle_t context_;
  ze_driver_handle_t driver_;

  std::vector<std::shared_ptr<L0Device>> devices_;

 public:
  explicit L0Driver(ze_driver_handle_t handle);
  ~L0Driver();
  ze_context_handle_t ctx() const;
  ze_driver_handle_t driver() const;

  const std::vector<std::shared_ptr<L0Device>>& devices() const;
};

class L0Module;
class L0Kernel;
class L0CommandList;

class L0Device {
 private:
  ze_device_handle_t device_;
  ze_command_queue_handle_t command_queue_;

  const L0Driver& driver_;

 public:
  L0Device(const L0Driver& driver, ze_device_handle_t device);

  ze_device_handle_t device() const;
  ze_command_queue_handle_t command_queue() const;
  std::unique_ptr<L0CommandList> create_command_list() const;
  ze_context_handle_t ctx() const;

  std::shared_ptr<L0Module> create_module(uint8_t* code,
                                          size_t len,
                                          bool log = false) const;

  ~L0Device();
};

class L0Module {
 private:
  ze_module_handle_t handle_;

 public:
  explicit L0Module(ze_module_handle_t handle);
  ze_module_handle_t handle() const;

  std::shared_ptr<L0Kernel> create_kernel(const char* name,
                                          uint32_t x,
                                          uint32_t y,
                                          uint32_t z) const;

  ~L0Module();
};

template <int Index>
void set_kernel_args(ze_kernel_handle_t kernel) {}

template <int Index, typename Head>
void set_kernel_args(ze_kernel_handle_t kernel, Head&& head) {
  L0_SAFE_CALL(zeKernelSetArgumentValue(
      kernel, Index, sizeof(std::remove_reference_t<Head>), head));
}

template <int Index, typename Head, typename... Tail>
void set_kernel_args(ze_kernel_handle_t kernel, Head&& head, Tail&&... tail) {
  set_kernel_args<Index>(kernel, head);
  set_kernel_args<Index + 1>(kernel, std::forward<Tail>(tail)...);
}

class L0Kernel {
 private:
  ze_kernel_handle_t handle_;
  ze_group_count_t group_size_;

 public:
  L0Kernel(ze_kernel_handle_t handle, uint32_t x, uint32_t y, uint32_t z);

  ze_group_count_t& group_size();
  ze_kernel_handle_t handle() const;
  ~L0Kernel();
};

class L0CommandList {
 private:
  ze_command_list_handle_t handle_;

 public:
  L0CommandList(ze_command_list_handle_t handle);

  void copy(void* dst, const void* src, const size_t num_bytes);

  template <typename... Args>
  void launch(L0Kernel& kernel, Args&&... args) {
    set_kernel_args<0>(kernel.handle(), std::forward<Args>(args)...);

    L0_SAFE_CALL(zeCommandListAppendLaunchKernel(
        handle_, kernel.handle(), &kernel.group_size(), nullptr, 0, nullptr));

    L0_SAFE_CALL(zeCommandListAppendBarrier(handle_, nullptr, 0, nullptr));
  }

  void submit(ze_command_queue_handle_t queue);

  ze_command_list_handle_t handle() const;

  ~L0CommandList();
};

void* allocate_device_mem(const size_t num_bytes, L0Device& device);

#endif  // HAVE_L0

class L0Manager {
 public:
  L0Manager();

  void copyHostToDevice(int8_t* device_ptr,
                        const int8_t* host_ptr,
                        const size_t num_bytes,
                        const int device_num);
  void copyDeviceToHost(int8_t* host_ptr,
                        const int8_t* device_ptr,
                        const size_t num_bytes,
                        const int device_num);
  void copyDeviceToDevice(int8_t* dest_ptr,
                          int8_t* src_ptr,
                          const size_t num_bytes,
                          const int dest_device_num,
                          const int src_device_num);

  int8_t* allocatePinnedHostMem(const size_t num_bytes);
  int8_t* allocateDeviceMem(const size_t num_bytes, const int device_num);
  void freePinnedHostMem(int8_t* host_ptr);
  void freeDeviceMem(int8_t* device_ptr);
  void zeroDeviceMem(int8_t* device_ptr, const size_t num_bytes, const int device_num);
  void setDeviceMem(int8_t* device_ptr,
                    const unsigned char uc,
                    const size_t num_bytes,
                    const int device_num);

  void synchronizeDevices() const;

#ifdef HAVE_L0
  const std::vector<std::shared_ptr<L0Driver>>& drivers() const;

 private:
  std::vector<std::shared_ptr<L0Driver>> drivers_;
#endif  // HAVE_L0
};

}  // namespace l0