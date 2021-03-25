#pragma once
#include <exception>

namespace l0 {
using L0result = int;

class L0Exception : public std::exception {
 public:
  L0Exception(L0result status);

  const char* what() const override;

 private:
  L0result const status_;
};
}