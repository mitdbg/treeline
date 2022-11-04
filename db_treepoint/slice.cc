#include "treeline/slice.h"

namespace std {

ostream& operator<<(ostream& os, const tl::Slice& slice) {
  std::string s;
  for (int i = 0; i < slice.size(); ++i) {
    s += "(";
    s += std::to_string(static_cast<uint8_t>(slice.data()[i]));
    s += ")";
  }
  os << s;
  return os;
}

}  // namespace std
