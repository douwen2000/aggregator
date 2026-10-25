#pragma once

// One published BookUpdate is shared by reference among all subscribers, so
// fanning out costs a refcount bump instead of a deep protobuf copy.
#include <memory>

#include "aggregator.pb.h"

namespace aggregator {

using BookUpdatePtr = std::shared_ptr<const BookUpdate>;

}  // namespace aggregator
