#include "model/model.h"

#include <memory>
#include <vector>

#include "db/page.h"
#include "gtest/gtest.h"
#include "llsm/options.h"
#include "llsm/status.h"
#include "model/direct_model.h"
#include "model/rs_model.h"
#include "util/key.h"

namespace {

using namespace llsm;

TEST(ModelTest, SerializeDirectModel) {
  KeyDistHints key_hints;
  key_hints.num_keys = 1000;
  std::unique_ptr<Model> model1(new DirectModel(key_hints));

  // Serialize.
  std::string buffer;
  model1->EncodeTo(&buffer);
  ASSERT_TRUE(buffer.size() > 0);

  // Deserialize.
  Status s;
  Slice payload(buffer);
  std::unique_ptr<Model> model2(Model::LoadFrom(&payload, &s));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(model2 != nullptr);

  // `Model::LoadFrom()` should advance the payload `Slice` past the parsed
  // model data.
  ASSERT_EQ(payload.size(), 0);

  // Make sure the two models make the same predictions.
  const std::vector<uint64_t> keys =
      key_utils::CreateValues<uint64_t>(key_hints);
  for (const uint64_t& key_as_int : keys) {
    const Slice key(reinterpret_cast<const char*>(&key_as_int),
                    sizeof(uint64_t));
    ASSERT_EQ(model1->KeyToPageId(key), model2->KeyToPageId(key));
  }
}

TEST(ModelTest, SerializeRSModel) {
  KeyDistHints key_hints;
  key_hints.num_keys = 1000;
  const auto values = key_utils::CreateValues<uint64_t>(key_hints);
  const auto records = key_utils::CreateRecords<uint64_t>(values);
  std::unique_ptr<Model> model1(new RSModel(key_hints, records));

  // Serialize.
  std::string buffer;
  model1->EncodeTo(&buffer);
  ASSERT_TRUE(buffer.size() > 0);

  // Deserialize.
  Status s;
  Slice payload(buffer);
  std::unique_ptr<Model> model2(Model::LoadFrom(&payload, &s));
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(model2 != nullptr);
  ASSERT_EQ(payload.size(), 0);

  // Make sure the two models make the same predictions.
  for (const auto& rec : records) {
    const Slice key(reinterpret_cast<const char*>(&rec.first),
                    sizeof(uint64_t));
    ASSERT_EQ(model1->KeyToPageId(key), model2->KeyToPageId(key));
  }
}

TEST(ModelTest, DeserializeUnknownModel) {
  KeyDistHints key_hints;
  key_hints.num_keys = 1000;
  std::unique_ptr<Model> model1(new DirectModel(key_hints));

  // Serialize.
  std::string buffer;
  model1->EncodeTo(&buffer);
  ASSERT_TRUE(buffer.size() > 0);

  // Purposely set the model type indicator byte to a unused value.
  buffer[0] = 255;

  // Deserialization should fail.
  Status s;
  Slice payload(buffer);
  std::unique_ptr<Model> model2(Model::LoadFrom(&payload, &s));
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_TRUE(model2 == nullptr);
}

TEST(ModelTest, DeserializeEmpty) {
  const std::string buffer;
  ASSERT_TRUE(buffer.empty());

  // Deserialization should fail.
  Status s;
  Slice payload(buffer);
  std::unique_ptr<Model> model(Model::LoadFrom(&payload, &s));
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_TRUE(model == nullptr);
}

}  // namespace
