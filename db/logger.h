#pragma once

#include <filesystem>
#include <memory>

#include "posix_logger.h"

namespace tl {

// A singleton logger class for debug and other informative logging.
// The `Log()` method is thread-safe. `Initialize()` and `Shutdown()` are not
// thread-safe and should not be called concurrently.
class Logger {
 public:
  // Log a message using printf-style formatting.
  static void Log(const char* format, ...) __attribute__((__format__(__printf__, 1, 2)));

  // Must be called first to enable logging. If this method is never called,
  // `Log()` calls will effectively be no-ops.
  static void Initialize(const std::filesystem::path& db_path);

  // Should be called when the database is shut down to stop logging.
  static void Shutdown();

 private:
  Logger();
  static Logger& Instance();

  std::unique_ptr<PosixLogger> logger_;
};

}  // namespace tl
