#include "BufferManager.hpp"

#include "AsyncWriteBuffer.hpp"
#include "BufferFrame.hpp"
#include "Exceptions.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/PPCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <iomanip>
#include <set>
// -------------------------------------------------------------------------------------
// Local GFlags
// -------------------------------------------------------------------------------------
using std::thread;
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
BufferManager::BufferManager(s32 ssd_fd) : ssd_fd(ssd_fd)
{
   // -------------------------------------------------------------------------------------
   // Init DRAM pool
   {
      dram_pool_size = FLAGS_dram_gib * 1024 * 1024 * 1024 / sizeof(BufferFrame);
      const u64 dram_total_size = sizeof(BufferFrame) * (dram_pool_size + safety_pages);
      bfs = reinterpret_cast<BufferFrame*>(mmap(NULL, dram_total_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
      madvise(bfs, dram_total_size, MADV_HUGEPAGE);
      madvise(bfs, dram_total_size,
              MADV_DONTFORK);  // O_DIRECT does not work with forking.
      // -------------------------------------------------------------------------------------
      // Initialize partitions
      partitions_count = (1 << FLAGS_partition_bits);
      partitions_mask = partitions_count - 1;
      const u64 free_bfs_limit = std::ceil((FLAGS_free_pct * 1.0 * dram_pool_size / 100.0) / static_cast<double>(partitions_count));
      const u64 cooling_bfs_upper_bound = std::ceil((FLAGS_cool_pct * 1.0 * dram_pool_size / 100.0) / static_cast<double>(partitions_count));
      partitions = reinterpret_cast<Partition*>(malloc(sizeof(Partition) * partitions_count));
      for (u64 p_i = 0; p_i < partitions_count; p_i++) {
         new (partitions + p_i) Partition(p_i, partitions_count, free_bfs_limit, cooling_bfs_upper_bound);
      }
      // -------------------------------------------------------------------------------------
      utils::Parallelize::parallelRange(dram_total_size, [&](u64 begin, u64 end) { memset(reinterpret_cast<u8*>(bfs) + begin, 0, end - begin); });
      utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
         u64 p_i = 0;
         for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
            partitions[p_i].dram_free_list.push(*new (bfs + bf_i) BufferFrame());
            p_i = (p_i + 1) % partitions_count;
         }
      });
      // -------------------------------------------------------------------------------------
   }
   // -------------------------------------------------------------------------------------
   // Page Provider threads
   if (FLAGS_pp_threads) {  // make it optional for pure in-memory experiments
      std::vector<thread> pp_threads;
      const u64 partitions_per_thread = partitions_count / FLAGS_pp_threads;
      ensure(FLAGS_pp_threads <= partitions_count);
      const u64 extra_partitions_for_last_thread = partitions_count % FLAGS_pp_threads;
      // -------------------------------------------------------------------------------------
      for (u64 t_i = 0; t_i < FLAGS_pp_threads; t_i++) {
         pp_threads.emplace_back(
             [&](u64 t_i, u64 p_begin, u64 p_end) {
                CPUCounters::registerThread("pp_" + std::to_string(t_i));
                // https://linux.die.net/man/2/setpriority
                if (FLAGS_root) {
                   posix_check(setpriority(PRIO_PROCESS, 0, -20) == 0);
                }
                pageProviderThread(p_begin, p_end);
             },
             t_i, t_i * partitions_per_thread,
             ((t_i + 1) * partitions_per_thread) + ((t_i == FLAGS_pp_threads - 1) ? extra_partitions_for_last_thread : 0));
         bg_threads_counter++;
      }
      for (u64 t_i = 0; t_i < FLAGS_pp_threads; t_i++) {
         thread& page_provider_thread = pp_threads[t_i];
         cpu_set_t cpuset;
         CPU_ZERO(&cpuset);
         CPU_SET(t_i, &cpuset);
         posix_check(pthread_setaffinity_np(page_provider_thread.native_handle(), sizeof(cpu_set_t), &cpuset) == 0);
         page_provider_thread.detach();
      }
   }
}
// -------------------------------------------------------------------------------------
void BufferManager::clearSSD()
{
   // TODO
}
// -------------------------------------------------------------------------------------
void BufferManager::writeAllBufferFrames()
{
   stopBackgroundThreads();
   ensure(!FLAGS_out_of_place);
   utils::Parallelize::parallelRange(dram_pool_size, [&](u64 bf_b, u64 bf_e) {
      BufferFrame::Page page;
      for (u64 bf_i = bf_b; bf_i < bf_e; bf_i++) {
         auto& bf = bfs[bf_i];
         bf.header.latch.mutex.lock();
         if (!bf.isFree()) {
            page.dt_id = bf.page.dt_id;
            page.magic_debugging_number = bf.header.pid;
            DTRegistry::global_dt_registry.checkpoint(bf.page.dt_id, bf, page.dt);
            //std::cerr << "MM: Writing out a buffer frame" << std::endl;
            s64 ret = pwrite(ssd_fd, page, PAGE_SIZE, bf.header.pid * PAGE_SIZE);
            ensure(ret == PAGE_SIZE);
         }
         bf.header.latch.mutex.unlock();
      }
      //std::cerr << "MM: Writeout worker done" << std::endl;
   });
}
// -------------------------------------------------------------------------------------
void BufferManager::restore()
{
   // TODO
}
// -------------------------------------------------------------------------------------
u64 BufferManager::consumedPages()
{
   u64 total_used_pages = 0, total_freed_pages = 0;
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      total_freed_pages += partitions[p_i].freedPages();
      total_used_pages += partitions[p_i].allocatedPages();
   }
   return total_used_pages - total_freed_pages;
}
// -------------------------------------------------------------------------------------
BufferFrame& BufferManager::getContainingBufferFrame(const u8* ptr)
{
   u64 index = (ptr - reinterpret_cast<u8*>(bfs)) / (sizeof(BufferFrame));
   return bfs[index];
}
// -------------------------------------------------------------------------------------
// Buffer Frames Management
// -------------------------------------------------------------------------------------
Partition& BufferManager::randomPartition()
{
   auto rand_partition_i = utils::RandomGenerator::getRand<u64>(0, partitions_count);
   return partitions[rand_partition_i];
}
// -------------------------------------------------------------------------------------
BufferFrame& BufferManager::randomBufferFrame()
{
   auto rand_buffer_i = utils::RandomGenerator::getRand<u64>(0, dram_pool_size);
   return bfs[rand_buffer_i];
}
// -------------------------------------------------------------------------------------
// returns a *write locked* new buffer frame
BufferFrame& BufferManager::allocatePage()
{
   // Pick a pratition randomly
   Partition& partition = randomPartition();
   BufferFrame& free_bf = partition.dram_free_list.pop();
   PID free_pid = partition.nextPID();
   assert(free_bf.header.state == BufferFrame::STATE::FREE);
   // -------------------------------------------------------------------------------------
   // Initialize Buffer Frame
   free_bf.header.latch.assertNotExclusivelyLatched();
   free_bf.header.latch.mutex.lock();  // Exclusive lock before changing to HOT
   free_bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT);
   free_bf.header.pid = free_pid;
   free_bf.header.state = BufferFrame::STATE::HOT;
   free_bf.header.lastWrittenGSN = free_bf.page.GSN = 0;
   // -------------------------------------------------------------------------------------
   if (free_pid == dram_pool_size) {
      cerr << "-------------------------------------------------------------------------------------" << endl;
      cerr << "Going out of memory !" << endl;
      cerr << "-------------------------------------------------------------------------------------" << endl;
   }
   free_bf.header.latch.assertExclusivelyLatched();
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK() { WorkerCounters::myCounters().allocate_operations_counter++; }
   // -------------------------------------------------------------------------------------
   return free_bf;
}
// -------------------------------------------------------------------------------------
// Pre: bf is exclusively locked
// ATTENTION: this function unlocks it !!
// -------------------------------------------------------------------------------------
void BufferManager::reclaimPage(BufferFrame& bf)
{
   Partition& partition = getPartition(bf.header.pid);
   partition.freePage(bf.header.pid);
   // -------------------------------------------------------------------------------------
   if (bf.header.isWB) {
      // DO NOTHING ! we have a garbage collector ;-)
      bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
      bf.header.latch.mutex.unlock();
      cerr << "garbage collector, yeah" << endl;
   } else {
      Partition& partition = getPartition(bf.header.pid);
      bf.reset();
      bf.header.latch->fetch_add(LATCH_EXCLUSIVE_BIT, std::memory_order_release);
      bf.header.latch.mutex.unlock();
      partition.dram_free_list.push(bf);
   }
}
// -------------------------------------------------------------------------------------
// returns a non-latched BufferFrame
BufferFrame& BufferManager::resolveSwip(Guard& swip_guard, Swip<BufferFrame>& swip_value)
{
   if (swip_value.isHOT()) {
      BufferFrame& bf = swip_value.bfRef();
      swip_guard.recheck();
      return bf;
   } else if (swip_value.isCOOL()) {
      BufferFrame* bf = swip_value.bfPtrAsHot();
      swip_guard.recheck();
      OptimisticGuard bf_guard(bf->header.latch, true);
      ExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);  // parent
      ExclusiveGuard bf_x_guard(bf_guard);                // child
      bf->header.state = BufferFrame::STATE::HOT;
      swip_value.warm();
      return swip_value.bfRef();
   }
   // -------------------------------------------------------------------------------------
   swip_guard.unlock();  // otherwise we would get a deadlock, P->G, G->P
   const PID pid = swip_value.asPageID();
   Partition& partition = getPartition(pid);
   JMUW<std::unique_lock<std::mutex>> g_guard(partition.io_mutex);
   swip_guard.recheck();
   assert(!swip_value.isHOT());
   // -------------------------------------------------------------------------------------
   auto frame_handler = partition.io_ht.lookup(pid);
   if (!frame_handler) {
      BufferFrame& bf = randomPartition().dram_free_list.tryPop(g_guard);  // EXP
      IOFrame& io_frame = partition.io_ht.insert(pid);
      assert(bf.header.state == BufferFrame::STATE::FREE);
      bf.header.latch.assertNotExclusivelyLatched();
      // -------------------------------------------------------------------------------------
      io_frame.state = IOFrame::STATE::READING;
      io_frame.readers_counter = 1;
      io_frame.mutex.lock();
      // -------------------------------------------------------------------------------------
      g_guard->unlock();
      // -------------------------------------------------------------------------------------
      readPageSync(pid, bf.page);
      COUNTERS_BLOCK()
      {
        // WorkerCounters::myCounters().dt_misses_counter[bf.page.dt_id]++;
         if (FLAGS_trace_dt_id >= 0 && bf.page.dt_id == FLAGS_trace_dt_id &&
             utils::RandomGenerator::getRand<u64>(0, FLAGS_trace_trigger_probability) == 0) {
            utils::printBackTrace();
         }
      }
      assert(bf.page.magic_debugging_number == pid);
      // -------------------------------------------------------------------------------------
      // ATTENTION: Fill the BF
      assert(!bf.header.isWB);
      bf.header.lastWrittenGSN = bf.page.GSN;
      bf.header.state = BufferFrame::STATE::LOADED;
      bf.header.pid = pid;
      // -------------------------------------------------------------------------------------
      jumpmuTry()
      {
         swip_guard.recheck();
         JMUW<std::unique_lock<std::mutex>> g_guard(partition.io_mutex);
         ExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);
         io_frame.mutex.unlock();
         swip_value.warm(&bf);
         bf.header.state = BufferFrame::STATE::HOT;  // ATTENTION: SET TO HOT AFTER
                                                     // IT IS SWIZZLED IN
         // -------------------------------------------------------------------------------------
         if (io_frame.readers_counter.fetch_add(-1) == 1) {
            partition.io_ht.remove(pid);
         }
         jumpmu_return bf;
      }
      jumpmuCatch()
      {
         // Change state to ready
         g_guard->lock();
         io_frame.bf = &bf;
         io_frame.state = IOFrame::STATE::READY;
         // -------------------------------------------------------------------------------------
         g_guard->unlock();
         io_frame.mutex.unlock();
         // -------------------------------------------------------------------------------------
         jumpmu::jump();
      }
   }
   // -------------------------------------------------------------------------------------
   IOFrame& io_frame = frame_handler.frame();
   // -------------------------------------------------------------------------------------
   if (io_frame.state == IOFrame::STATE::READING) {
      io_frame.readers_counter++;  // incremented while holding partition lock
      g_guard->unlock();
      io_frame.mutex.lock();
      io_frame.mutex.unlock();
      if (io_frame.readers_counter.fetch_add(-1) == 1) {
         g_guard->lock();
         if (io_frame.readers_counter == 0) {
            partition.io_ht.remove(pid);
         }
         g_guard->unlock();
      }
      // -------------------------------------------------------------------------------------
      jumpmu::jump();
   }
   // -------------------------------------------------------------------------------------
   if (io_frame.state == IOFrame::STATE::READY) {
      // -------------------------------------------------------------------------------------
      BufferFrame* bf = io_frame.bf;
      {
         // We have to exclusively lock the bf because the page provider thread will
         // try to evict them when its IO is done
         bf->header.latch.assertNotExclusivelyLatched();
         assert(bf->header.state == BufferFrame::STATE::LOADED);
         OptimisticGuard bf_guard(bf->header.latch);
         ExclusiveUpgradeIfNeeded swip_x_guard(swip_guard);
         ExclusiveGuard bf_x_guard(bf_guard);
         // -------------------------------------------------------------------------------------
         io_frame.bf = nullptr;
         assert(bf->header.pid == pid);
         swip_value.warm(bf);
         assert(swip_value.isHOT());
         assert(bf->header.state == BufferFrame::STATE::LOADED);
         bf->header.state = BufferFrame::STATE::HOT;  // ATTENTION: SET TO HOT AFTER
                                                      // IT IS SWIZZLED IN
         // -------------------------------------------------------------------------------------
         if (io_frame.readers_counter.fetch_add(-1) == 1) {
            partition.io_ht.remove(pid);
         } else {
            io_frame.state = IOFrame::STATE::TO_DELETE;
         }
         g_guard->unlock();
         // -------------------------------------------------------------------------------------
         return *bf;
      }
   }
   if (io_frame.state == IOFrame::STATE::TO_DELETE) {
      if (io_frame.readers_counter == 0) {
         partition.io_ht.remove(pid);
      }
      g_guard->unlock();
      jumpmu::jump();
   }
   ensure(false);
}  // namespace storage
// -------------------------------------------------------------------------------------
// SSD management
// -------------------------------------------------------------------------------------
void BufferManager::readPageSync(u64 pid, u8* destination)
{
   assert(u64(destination) % kIOAlignment == 0);
   s64 bytes_left = PAGE_SIZE;
   do {
      const int bytes_read = pread(ssd_fd, destination, bytes_left, pid * PAGE_SIZE + (PAGE_SIZE - bytes_left));
      assert(bytes_left > 0);
      bytes_left -= bytes_read;
   } while (bytes_left > 0);
   // -------------------------------------------------------------------------------------
   COUNTERS_BLOCK() { WorkerCounters::myCounters().read_operations_counter++; }
}
// -------------------------------------------------------------------------------------
void BufferManager::fDataSync()
{
   fdatasync(ssd_fd);
}
// -------------------------------------------------------------------------------------
u64 BufferManager::getPartitionID(PID pid)
{
   return pid & partitions_mask;
}
// -------------------------------------------------------------------------------------
Partition& BufferManager::getPartition(PID pid)
{
   const u64 partition_i = getPartitionID(pid);
   assert(partition_i < partitions_count);
   return partitions[partition_i];
}
// -------------------------------------------------------------------------------------
void BufferManager::stopBackgroundThreads()
{
   bg_threads_keep_running = false;
   while (bg_threads_counter) {
      MYPAUSE();
   }
}
// -------------------------------------------------------------------------------------
BufferManager::~BufferManager()
{
   stopBackgroundThreads();
   free(partitions);
   // -------------------------------------------------------------------------------------
   const u64 dram_total_size = sizeof(BufferFrame) * (dram_pool_size + safety_pages);
   munmap(bfs, dram_total_size);
}
// -------------------------------------------------------------------------------------
// State
std::unordered_map<std::string, std::string> BufferManager::serialize()
{
   // TODO: correctly serialize ranges of used pages
   std::unordered_map<std::string, std::string> map;
   PID max_pid = 0;
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      max_pid = std::max<PID>(getPartition(p_i).next_pid, max_pid);
   }
   map["max_pid"] = std::to_string(max_pid);
   return map;
}
// -------------------------------------------------------------------------------------
void BufferManager::deserialize(std::unordered_map<std::string, std::string> map)
{
   PID max_pid = std::stod(map["max_pid"]);
   max_pid = (max_pid + (partitions_count - 1)) & ~(partitions_count - 1);
   for (u64 p_i = 0; p_i < partitions_count; p_i++) {
      getPartition(p_i).next_pid = max_pid + p_i;
   }
}
// -------------------------------------------------------------------------------------
BufferManager* BMC::global_bf(nullptr);
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
