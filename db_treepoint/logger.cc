#include "logger.h"

#include <cassert>

namespace tl {

void Logger::Initialize(const std::filesystem::path& db_path) {
  Logger& logger = Instance();
  logger.logger_ =
      std::make_unique<PosixLogger>(std::fopen((db_path / "LOG").c_str(), "a"));
}

void Logger::Shutdown() {
  Logger& logger = Instance();
  if (logger.logger_ == nullptr) return;
  logger.logger_.reset(nullptr);
}

Logger::Logger() : logger_(nullptr) {}

Logger& Logger::Instance() {
  static Logger logger;
  return logger;
}

void Logger::Log(const char* format, ...) {
  Logger& logger = Instance();
  if (logger.logger_ == nullptr) return;

  std::va_list ap;
  va_start(ap, format);
  logger.logger_->Logv(format, ap);
  va_end(ap);
}

}  // namespace tl
