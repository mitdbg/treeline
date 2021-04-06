#include "bufmgr/logical_page_id.h"

namespace std {

ostream& operator<<(ostream& os, const llsm::LogicalPageId& id) {
  os << id.IsOverflow() << "-" << id.GetRawPageId();
  return os;
}

}  // namespace std