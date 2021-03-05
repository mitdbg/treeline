#include "model.h"

#include "llsm/slice.h"
#include "llsm/status.h"
#include "model/direct_model.h"
#include "model/rs_model.h"

namespace llsm {

using detail::ModelType;

std::unique_ptr<Model> Model::LoadFrom(Slice* source, Status* status_out) {
  if (source->size() == 0) {
    *status_out = Status::InvalidArgument("Empty source buffer.");
    return nullptr;
  }

  const uint8_t raw_model_type = static_cast<uint8_t>((*source)[0]);
  if (raw_model_type > detail::kMaxModelType) {
    *status_out = Status::InvalidArgument("Unrecognized model type:",
                                          std::to_string(raw_model_type));
    return nullptr;
  }

  const ModelType model_type = static_cast<ModelType>(raw_model_type);
  if (model_type == ModelType::kDirectModel) {
    return DirectModel::LoadFrom(source, status_out);
  } else if (model_type == ModelType::kRSModel) {
    return RSModel::LoadFrom(source, status_out);
  } else {
    *status_out = Status::InvalidArgument("Unreachable code.");
    return nullptr;
  }
}

}  // namespace llsm
