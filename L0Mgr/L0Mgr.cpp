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

#include "L0Mgr/L0Mgr.h"

#include "Logger/Logger.h"
#include "Utils.h"

#include <iostream>
#include <limits>

#include <level_zero/ze_api.h>

namespace l0 {

L0Driver::L0Driver(ze_driver_handle_t handle) : driver_(handle) {
  ze_context_desc_t ctx_desc = {ZE_STRUCTURE_TYPE_CONTEXT_DESC, nullptr, 0};
  L0_SAFE_CALL(zeContextCreate(driver_, &ctx_desc, &context_));

  uint32_t device_count = 0;
  L0_SAFE_CALL(zeDeviceGet(driver_, &device_count, nullptr));

  std::vector<ze_device_handle_t> devices(device_count);
  L0_SAFE_CALL(zeDeviceGet(driver_, &device_count, devices.data()));

  for (auto device : devices) {
    ze_device_properties_t device_properties;
    L0_SAFE_CALL(zeDeviceGetProperties(device, &device_properties));
    if (ZE_DEVICE_TYPE_GPU == device_properties.type) {
      devices_.push_back(std::make_shared<L0Device>(*this, device));
    }
  }
}

L0Driver::~L0Driver() {
  auto status = (zeContextDestroy(context_));
  if (status) {
    std::cerr << "Non-zero status for context destructor" << std::endl;
  }
}

ze_context_handle_t L0Driver::ctx() const {
  return context_;
}

ze_driver_handle_t L0Driver::driver() const {
  return driver_;
}

const std::vector<std::shared_ptr<L0Device>>& L0Driver::devices() const {
  return devices_;
}

std::vector<std::shared_ptr<L0Driver>> get_drivers() {
  zeInit(0);
  uint32_t driver_count = 0;
  zeDriverGet(&driver_count, nullptr);

  std::vector<ze_driver_handle_t> handles(driver_count);
  zeDriverGet(&driver_count, handles.data());

  std::vector<std::shared_ptr<L0Driver>> result(driver_count);
  for (int i = 0; i < driver_count; i++) {
    result[i] = std::make_shared<L0Driver>(handles[i]);
  }
  return result;
}

L0CommandList::L0CommandList(ze_command_list_handle_t handle) : handle_(handle) {}

void L0CommandList::submit(ze_command_queue_handle_t queue) {
  L0_SAFE_CALL(zeCommandListClose(handle_));
  L0_SAFE_CALL(zeCommandQueueExecuteCommandLists(queue, 1, &handle_, nullptr));
}

void L0CommandList::launch(L0Kernel& kernel) {
  L0_SAFE_CALL(zeCommandListAppendLaunchKernel(
      handle_, kernel.handle(), &kernel.group_size(), nullptr, 0, nullptr));

  L0_SAFE_CALL(zeCommandListAppendBarrier(handle_, nullptr, 0, nullptr));
}
L0CommandList::~L0CommandList() {
  // TODO: maybe return to pool
}

void L0CommandList::copy(void* dst, const void* src, const size_t num_bytes) {
  L0_SAFE_CALL(
      zeCommandListAppendMemoryCopy(handle_, dst, src, num_bytes, nullptr, 0, nullptr));
  L0_SAFE_CALL(zeCommandListAppendBarrier(handle_, nullptr, 0, nullptr));
}

void* allocate_device_mem(const size_t num_bytes, L0Device& device) {
  ze_device_mem_alloc_desc_t alloc_desc;
  alloc_desc.stype = ZE_STRUCTURE_TYPE_DEVICE_MEM_ALLOC_DESC;
  alloc_desc.pNext = nullptr;
  alloc_desc.flags = 0;
  alloc_desc.ordinal = 0;

  void* mem;
  L0_SAFE_CALL(zeMemAllocDevice(
      device.ctx(), &alloc_desc, num_bytes, 0 /*align*/, device.device(), &mem));
  return mem;
}

L0Device::L0Device(const L0Driver& driver, ze_device_handle_t device)
    : device_(device), driver_(driver) {
  ze_command_queue_desc_t command_queue_desc = {ZE_STRUCTURE_TYPE_COMMAND_QUEUE_DESC,
                                                nullptr,
                                                0,
                                                0,
                                                0,
                                                ZE_COMMAND_QUEUE_MODE_DEFAULT,
                                                ZE_COMMAND_QUEUE_PRIORITY_NORMAL};
  L0_SAFE_CALL(
      zeCommandQueueCreate(driver_.ctx(), device_, &command_queue_desc, &command_queue_));
}

L0Device::~L0Device() {
  auto status = (zeCommandQueueDestroy(command_queue_));
  if (status) {
    std::cerr << "Non-zero status for command queue destructor" << std::endl;
  }
}

ze_context_handle_t L0Device::ctx() const {
  return driver_.ctx();
}
ze_device_handle_t L0Device::device() const {
  return device_;
}
ze_command_queue_handle_t L0Device::command_queue() const {
  return command_queue_;
}

std::unique_ptr<L0CommandList> L0Device::create_command_list() const {
  ze_command_list_desc_t desc = {
      ZE_STRUCTURE_TYPE_COMMAND_LIST_DESC,
      nullptr,
      0,
      0  // flags
  };
  ze_command_list_handle_t res;
  zeCommandListCreate(ctx(), device_, &desc, &res);
  return std::make_unique<L0CommandList>(res);
}

std::shared_ptr<L0Module> L0Device::create_module(uint8_t* code, size_t len) const {
  ze_module_desc_t desc{
      .stype = ZE_STRUCTURE_TYPE_MODULE_DESC,
      .pNext = nullptr,
      .format = ZE_MODULE_FORMAT_IL_SPIRV,
      .inputSize = len,
      .pInputModule = code,
      .pBuildFlags = "",
      .pConstants = nullptr,
  };
  ze_module_handle_t handle;
  L0_SAFE_CALL(zeModuleCreate(ctx(), device_, &desc, &handle, nullptr));
  return std::make_shared<L0Module>(handle);
}

L0Manager::L0Manager() : drivers_(get_drivers()) {}

const std::vector<std::shared_ptr<L0Driver>>& L0Manager::drivers() const {
  return drivers_;
}

L0Module::L0Module(ze_module_handle_t handle) : handle_(handle) {}

ze_module_handle_t L0Module::handle() const {
  return handle_;
}

L0Module::~L0Module() {
  auto status = zeModuleDestroy(handle_);
  if (status) {
    std::cerr << "Non-zero status for command module destructor" << std::endl;
  }
}

ze_group_count_t& L0Kernel::group_size() {
  return group_size_;
}

ze_kernel_handle_t L0Kernel::handle() const {
  return handle_;
}

L0Kernel::~L0Kernel() {
  auto status = zeKernelDestroy(handle_);
  if (status) {
    std::cerr << "Non-zero status for command kernel destructor" << std::endl;
  }
}
}  // namespace l0