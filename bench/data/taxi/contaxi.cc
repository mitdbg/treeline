#include <fstream>
#include <iostream>
#include <unordered_set>
#include <vector>

#include "../common.h"
#include "s2/s2cell_id.h"
#include "s2/s2latlng.h"

bool parse_line(std::ifstream& in, std::string& line,
                std::vector<std::pair<const char*, const char*>>& elements) {
  if (!std::getline(in, line)) return false;
  const char* begin = line.data();
  const char* limit = begin + line.length();
  if ((begin != limit) && (limit[-1] == ',')) --limit;
  const char* last = begin;
  elements.clear();
  for (auto iter = begin; iter != limit; ++iter) {
    if ((*iter) == ',') {
      elements.push_back(std::make_pair(last, iter));
      last = iter + 1;
    }
  }
  elements.push_back(std::make_pair(last, limit));
  return true;
}

double parse_double(const std::pair<const char*, const char*>& element) {
  return strtod(element.first, nullptr);
}

// Reads in lng/lat coordinates from a CSV file and converts them to S2 cell ids
// (64-bit unsigned integers). S2 cell ids represent points on a Hilbert curve
// (which preserves spatial locality). Zeros out most significant 16 bits (for
// compatibility with our benchmarking framework).

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: ./contaxi <filename>" << std::endl;
    exit(EXIT_FAILURE);
  }

  const std::string filename = argv[1];

  // Read lat/lng coordinates from file and convert to S2 cell ids.
  std::vector<uint64_t> cell_ids;

  std::ifstream in(filename);
  if (!in.is_open()) {
    std::cerr << "unable to open " << filename << std::endl;
    exit(-1);
  }

  std::string line;
  std::vector<std::pair<const char*, const char*>> elements;
  while (parse_line(in, line, elements)) {
    const double latitude = parse_double(elements[1]);
    const double longitude = parse_double(elements[0]);

    // Compute S2 cell id.
    const S2LatLng lat_lng =
        S2LatLng::FromDegrees(latitude, longitude).Normalized();
    const uint64_t cell_id = S2CellId(lat_lng.ToPoint()).id();

    cell_ids.push_back(cell_id);
  }

  // Remove duplicates while retaining the original sort order.
  std::vector<size_t> cell_ids_unique;
  cell_ids_unique.reserve(cell_ids.size());
  std::unordered_set<uint64_t> set;
  set.reserve(cell_ids.size());
  for (const uint64_t cell_id : cell_ids) {
    // Zero out most significant 16 bits.
    const uint64_t zerod_cell_id = (cell_id << 16) >> 16;
    if (set.find(zerod_cell_id) == set.end()) {
      set.insert(zerod_cell_id);
      cell_ids_unique.push_back(zerod_cell_id);
    }
  }

  std::cout << "Number of unique cell ids: " << cell_ids_unique.size()
            << std::endl;

  std::vector<Record> records;  // S2 cell id, value pairs
  records.reserve(cell_ids_unique.size());
  for (uint64_t i = 0; i < cell_ids_unique.size(); ++i) {
    records.push_back({cell_ids_unique[i], i});
  }

  // Write records to CSV file.
  const std::string out_filename = filename + "_trace_keys.csv";
  write_to_csv(records, out_filename, /*key_only=*/true);

  return 0;
}
