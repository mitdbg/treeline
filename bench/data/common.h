#include <fstream>
#include <string>
#include <vector>

struct Record {
  uint64_t key;
  uint64_t value;
};

// Loads values from binary file into vector.
template <typename T>
static std::vector<T> load_data(const std::string& filename) {
  std::vector<T> data;

  std::ifstream in(filename, std::ios::binary);
  if (!in.is_open()) {
    std::cerr << "Unable to open " << filename << std::endl;
    exit(EXIT_FAILURE);
  }
  // Read size.
  uint64_t size;
  in.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
  data.resize(size);
  // Read values.
  in.read(reinterpret_cast<char*>(data.data()), size * sizeof(T));
  in.close();

  std::cout << "Read " << data.size() << " values from " << filename
            << std::endl;

  return data;
}

// Writes `records` to a CSV file.
static void write_to_csv(const std::vector<Record>& records,
                         const std::string& filename,
                         const bool key_only = false) {
  std::cout << "Writing records to CSV file " << filename << "...";
  std::ofstream out(filename, std::ios_base::trunc);
  if (!out.is_open()) {
    std::cerr << "Unable to open " << filename << std::endl;
    exit(EXIT_FAILURE);
  }
  for (const auto& record : records) {
    if (key_only) {
      out << record.key << std::endl;
    } else {
      out << record.key << "," << record.value << std::endl;
    }
  }
  out.close();

  std::cout << " done." << std::endl;
}
