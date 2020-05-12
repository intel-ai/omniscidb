#pragma once

#include <functional>
#include <future>

#include <tbb/parallel_for.h>
#include <tbb/parallel_reduce.h>
#include <tbb/blocked_range.h>
#include <tbb/task_arena.h>
#include <tbb/task_group.h>

#include <arrow/util/task_group.h>
#include <arrow/util/thread_pool.h>

namespace utils {

using tbb::blocked_range;  // todo
using tbb::task_group;     // todo
using tbb::task_arena;     // todo
namespace this_task_arena {
using namespace tbb::this_task_arena;
}

extern tbb::task_arena g_tbb_arena;  // todo ?

namespace tbb_threading {

template<typename... X>
void parallel_for(X && ... x) {
  g_tbb_arena.execute([&] {
    this_task_arena::isolate([&] {
      tbb::parallel_for(std::forward<X>(x)...);
  }); });
}

template<typename... X >
auto parallel_reduce(X && ... x)
  -> decltype( tbb::parallel_reduce(std::forward<X>(x)...) ) {
  return g_tbb_arena.execute([&] {
    return this_task_arena::isolate([&] {
        return tbb::parallel_reduce(std::forward<X>(x)...);
  }); });
}

// // // // // // // // // // // // // // // // //

struct tbb_packaged_task_base {
  tbb::task *wtask_;
  tbb_packaged_task_base() {
    wtask_ = new(tbb::task::allocate_root()) tbb::empty_task;
    wtask_->set_ref_count(1); //wait task
  }
  ~tbb_packaged_task_base() {
    wtask_->set_ref_count(0);
    tbb::task::destroy(*wtask_);
  }
};

template<typename T>
struct tbb_packaged_task : tbb_packaged_task_base {
  T value_;
  tbb_packaged_task() : value_(T()) {}
};

template<>
struct tbb_packaged_task<void> : tbb_packaged_task_base {
};

template<typename T>
struct future {
  std::unique_ptr<tbb_packaged_task<T>> task_;
  future() = default;
  future(future &&) = default;
  future(std::unique_ptr<tbb_packaged_task<T>> &&p) : task_(std::move(p)) {}
  void wait() {
    g_tbb_arena.execute( [this]{
      task_->wtask_->wait_for_all();
      task_->wtask_->set_ref_count(1);
    });
  }
  T &get() { wait(); return task_->value_; }
};

template<>
struct future<void> {
  std::unique_ptr<tbb_packaged_task<void>> task_;
  future() = default;
  future(future &&) = default;
  future(std::unique_ptr<tbb_packaged_task<void>> &&p) : task_(std::move(p)) {}
  void wait() {
    g_tbb_arena.execute( [this]{
      task_->wtask_->wait_for_all();
      task_->wtask_->set_ref_count(1);
    });
  }
  void get() { wait(); }
};

template <typename Fn, typename R = std::result_of_t<Fn && ()>>
struct tbb_task_handle : public tbb::task {
  tbb_packaged_task<R> *res_;
  Fn fn_;

  tbb_task_handle(tbb_packaged_task<R> *r, Fn &&v) :  res_(r), fn_(std::move(v)) {}
  tbb::task* execute() override {
    /*tbb::this_task_arena::isolate([this] {*/ res_->value_ = fn_(); //});
    return NULL;
  }
};

template <typename Fn>
struct tbb_task_handle<Fn, void> : public tbb::task {
  Fn fn_;

  tbb_task_handle(tbb_packaged_task<void> *, Fn &&v) :  fn_(std::move(v)) {}
  tbb::task* execute() override {
    /*tbb::this_task_arena::isolate([this] {*/ fn_(); //});
    return NULL;
  }
};

template <typename Fn,
          typename... Args,
          typename Result = std::result_of_t<Fn && (Args && ...)>>
future<Result> async(Fn&& fn, Args&&... args) {
  auto f = std::bind(std::forward<Fn>(fn), std::forward<Args>(args)...);
  auto ptask = std::make_unique<tbb_packaged_task<Result>>();
  tbb::task::enqueue(*new(tbb::task::allocate_additional_child_of(*ptask->wtask_))
                      tbb_task_handle<decltype(f)>(ptask.get(), std::move(f)),
                      g_tbb_arena);
  return future<Result>(std::move(ptask));
}

} // namespace tbb_threading

namespace arrow_threading {

using std::future;
using arrow::internal::GetCpuThreadPool;
using arrow::internal::TaskGroup;
using arrow::internal::ThreadPool;

template <typename Fn,
          typename... Args,
          typename Result = std::result_of_t<Fn && (Args && ...)>>
std::future<Result> async(Fn&& fn, Args&&... args) {
  auto pool = GetCpuThreadPool();
  //return pool->Submit(std::forward<Fn>(fn), std::forward<Args>(args)...);
  using PackagedTask = std::packaged_task<Result()>;
  auto task = PackagedTask(std::bind(std::forward<Fn>(fn), std::forward<Args>(args)...));
  auto fut = task.get_future();

  auto st = pool->Spawn(
      arrow::internal::detail::packaged_task_wrapper<Result>(std::move(task)));
  //CHECK(st.ok()) << st.ToString();
  return fut;
}
}  // namespace arrow_threading

#if ENABLE_TBB
using tbb_threading::parallel_for;
using tbb_threading::parallel_reduce;
using tbb_threading::async;
using tbb_threading::future;
#else
using std::async;
using std::future;
//todo
#endif
}  // namespace utils
