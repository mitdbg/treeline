#include "db/manifest.h"

#include <filesystem>
#include <fstream>
#include <optional>

#include "gtest/gtest.h"
#include "treeline/status.h"
#include "util/coding.h"

namespace {

using namespace tl;
namespace fs = std::filesystem;

class ManifestTest : public testing::Test {
 public:
  const fs::path kManifestFile =
      "/tmp/tl-test/MANIFEST-" + std::to_string(std::time(nullptr));

  void SetUp() override {
    fs::remove_all(kManifestFile.parent_path());
    fs::create_directory(kManifestFile.parent_path());
  }

  void TearDown() override { fs::remove_all(kManifestFile.parent_path()); }
};

TEST_F(ManifestTest, SimpleWriteRead) {
  const uint64_t num_pages = 100;
  const uint64_t num_segments = 10;

  Status s = Manifest::Builder()
                 .WithNumSegments(num_segments)
                 .WithNumPages(num_pages)
                 .Build()
                 .WriteTo(kManifestFile);
  ASSERT_TRUE(s.ok());

  const std::optional<Manifest> manifest =
      Manifest::LoadFrom(kManifestFile, &s);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(manifest.has_value());

  ASSERT_EQ(manifest->num_pages(), num_pages);
  ASSERT_EQ(manifest->num_segments(), num_segments);
}

TEST_F(ManifestTest, HeaderCorruption) {
  Status s = Manifest::Builder()
                 .WithNumSegments(1000)
                 .WithNumPages(10000)
                 .Build()
                 .WriteTo(kManifestFile);
  ASSERT_TRUE(s.ok());

  // Overwrite the manifest signature.
  {
    // The `in` and `out` flags are required to prevent the `fstream` from
    // truncating the file.
    std::fstream file(kManifestFile, std::ios_base::in | std::ios_base::out |
                                         std::ios_base::binary);
    file.write(u8"ABCD", 4);
    ASSERT_FALSE(file.fail());
  }

  const std::optional<Manifest> manifest =
      Manifest::LoadFrom(kManifestFile, &s);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_FALSE(manifest.has_value());
}

TEST_F(ManifestTest, PayloadCorruption) {
  Status s = Manifest::Builder()
                 .WithNumSegments(1000)
                 .WithNumPages(10000)
                 .Build()
                 .WriteTo(kManifestFile);
  ASSERT_TRUE(s.ok());

  // Overwrite the last four bytes.
  {
    // The `in` and `out` flags are required to prevent the `fstream` from
    // truncating the file.
    std::fstream file(kManifestFile, std::ios_base::in | std::ios_base::out |
                                         std::ios_base::binary);
    file.seekp(-4, std::ios_base::end);
    file.write(u8"ABCD", 4);
    ASSERT_FALSE(file.fail());
  }

  const std::optional<Manifest> manifest =
      Manifest::LoadFrom(kManifestFile, &s);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_FALSE(manifest.has_value());
}

TEST_F(ManifestTest, ReadNonexistent) {
  Status s;
  std::optional<Manifest> maybe_loaded = Manifest::LoadFrom(kManifestFile, &s);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_FALSE(maybe_loaded.has_value());
}

TEST_F(ManifestTest, UnsupportedFormatVersion) {
  Status s = Manifest::Builder()
                 .WithNumSegments(1000)
                 .WithNumPages(10000)
                 .Build()
                 .WriteTo(kManifestFile);
  ASSERT_TRUE(s.ok());

  // Set the format version to 0.
  {
    // The `in` and `out` flags are required to prevent the `fstream` from
    // truncating the file.
    std::fstream file(kManifestFile, std::ios_base::in | std::ios_base::out |
                                         std::ios_base::binary);
    file.seekp(2, std::ios_base::beg);
    char buf[4];
    EncodeFixed32(buf, 0);
    file.write(buf, 4);
    ASSERT_FALSE(file.fail());
  }

  const std::optional<Manifest> manifest =
      Manifest::LoadFrom(kManifestFile, &s);
  ASSERT_TRUE(s.IsNotSupportedError());
  ASSERT_FALSE(manifest.has_value());
}

}  // namespace
