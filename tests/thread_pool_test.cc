#include "util/thread_pool.h"

#include <stdexcept>

#include "gtest/gtest.h"

static int Square(int val) { return val * val; }

static int SafeIntDivide(int a, int b) {
  if (b == 0) throw std::invalid_argument("Division by zero.");
  return a / b;
}

TEST(ThreadPoolTest, Submit) {
  tl::ThreadPool pool(2);
  auto f1 = pool.Submit(Square, 5);
  auto f2 = pool.Submit(Square, 1);
  auto f3 = pool.Submit(Square, 10);
  ASSERT_EQ(f1.get(), 25);
  ASSERT_EQ(f2.get(), 1);
  ASSERT_EQ(f3.get(), 100);
}

TEST(ThreadPoolTest, SubmitWithException) {
  tl::ThreadPool pool(3);
  auto f1 = pool.Submit(SafeIntDivide, 5, 2);
  auto f2 = pool.Submit(SafeIntDivide, 3, 0);
  auto f3 = pool.Submit(SafeIntDivide, 10, 2);
  ASSERT_EQ(f1.get(), 2);
  ASSERT_EQ(f3.get(), 5);  // Can retrieve results out of order
  ASSERT_THROW(f2.get(), std::invalid_argument);

  auto f4 = pool.Submit(Square, 11);
  auto f5 = pool.Submit(SafeIntDivide, 100, 10);
  ASSERT_EQ(f4.get(), 121);
  ASSERT_EQ(f5.get(), 10);
}

TEST(ThreadPoolTest, SubmitNoWait) {
  int val1 = 1;
  int val2 = 10;
  {
    tl::ThreadPool pool(2);
    pool.SubmitNoWait([&val1]() { val1 += 1; });
    pool.SubmitNoWait([&val2]() { val2 *= val2; });
  }
  ASSERT_EQ(val1, 2);
  ASSERT_EQ(val2, 100);
}
