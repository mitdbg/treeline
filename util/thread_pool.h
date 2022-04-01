#pragma once

#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <type_traits>
#include <vector>

namespace tl {

// A thread pool that supports thread-to-core pinning.
//
// Acknowledgements: This implementation is based on other existing thread pools
//   - https://github.com/fbastos1/thread_pool_cpp17
//   - https://github.com/progschj/ThreadPool
//   - https://github.com/vit-vit/CTPL
class ThreadPool {
 public:
  // Create a thread pool with `num_threads` threads. Threads will run
  // `run_on_exit` just before they terminate.
  ThreadPool(size_t num_threads,
             std::function<void()> run_on_exit = std::function<void()>());

  // Create a thread pool with `num_threads` threads and pin each thread to the
  // core id specified by `thread_to_core`.
  //
  // The `thread_to_core` vector must be of size `num_threads`. The value at
  // `thread_to_core[i]` represents the core id that thread `i` should be pinned
  // to, where `0 <= i < num_threads`.
  ThreadPool(size_t num_threads, const std::vector<size_t>& thread_to_core);

  // Waits for all submitted functions to execute before returning.
  ~ThreadPool();

  // Schedule `f(...args)` to run on a thread in this thread pool.
  //
  // This method returns a `std::future` that can be used to wait for `f` to
  // run and to retrieve its return value (if any).
  template <typename Function, typename... Args,
            std::enable_if_t<std::is_invocable<Function&&, Args&&...>::value,
                             bool> = true>
  auto Submit(Function&& f, Args&&... args);

  // Similar to `Submit()`, but instead does not provide a future that can be
  // used to wait on the function's result.
  template <typename Function, typename... Args,
            std::enable_if_t<std::is_invocable<Function&&, Args&&...>::value,
                             bool> = true>
  void SubmitNoWait(Function&& f, Args&&... args);

 private:
  class Task {
   public:
    virtual ~Task() = default;
    virtual void operator()() = 0;
  };

  template <typename Function>
  class TaskContainer : public Task {
   public:
    TaskContainer(Function&& f) : f_(std::forward<Function>(f)) {}
    void operator()() override { f_(); }

   private:
    Function f_;
  };

  // Worker threads run this code.
  void ThreadMain();

  // Ensures the worker thread runs `ThreadMain()` on core `core_id`.
  void ThreadMainOnCore(size_t core_id);

  std::mutex mutex_;
  std::condition_variable cv_;
  bool shutdown_;
  std::queue<std::unique_ptr<Task>> work_queue_;
  std::vector<std::thread> threads_;
  std::function<void()> run_on_exit_;
};

template <
    typename Function, typename... Args,
    std::enable_if_t<std::is_invocable<Function&&, Args&&...>::value, bool>>
auto ThreadPool::Submit(Function&& f, Args&&... args) {
  std::packaged_task<std::invoke_result_t<Function, Args...>()> task(
      [runnable = std::move(f),
       task_args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
        return std::apply(std::move(runnable), std::move(task_args));
      });
  auto future = task.get_future();
  {
    std::unique_lock<std::mutex> lock(mutex_);
    work_queue_.emplace(new TaskContainer(std::move(task)));
  }
  cv_.notify_one();
  return future;
}

template <
    typename Function, typename... Args,
    std::enable_if_t<std::is_invocable<Function&&, Args&&...>::value, bool>>
void ThreadPool::SubmitNoWait(Function&& f, Args&&... args) {
  auto task = [runnable = std::move(f),
               task_args =
                   std::make_tuple(std::forward<Args>(args)...)]() mutable {
    std::apply(std::move(runnable), std::move(task_args));
  };
  {
    std::unique_lock<std::mutex> lock(mutex_);
    work_queue_.emplace(new TaskContainer(std::move(task)));
  }
  cv_.notify_one();
}

}  // namespace tl
