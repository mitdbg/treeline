  #pragma once
  
  #include <cstdlib>
  
  namespace tl {
  
  // Find the smallest power of 2 larger than or equal to `num`
  inline size_t Pow2Ceil(size_t num) {
    size_t count = 0;

    if (num && !(num & (num - 1))) return num;

    while (num > 0) {
      num >>= 1;
      ++count;
    }

    return 1ULL << count;
  }

  } // namespace tl
