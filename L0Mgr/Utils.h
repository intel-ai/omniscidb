#pragma once
#include "L0Exception.h"

#define L0_SAFE_CALL(call)       \
  {                              \
    auto status = (call);        \
    if (status) {                \
      throw L0Exception(status); \
    }                            \
  }