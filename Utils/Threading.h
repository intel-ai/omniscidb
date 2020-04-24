#pragma once

#include <arrow/util/task_group.h>
#include <arrow/util/thread_pool.h>
#include <future>

namespace utils {
using arrow::internal::GetCpuThreadPool;
using arrow::internal::TaskGroup;
using arrow::internal::ThreadPool;

template <typename Fn,
          typename... Args,
          typename Result = std::result_of_t<Fn && (Args && ...)>>
std::future<Result> async(Fn&& fn, Args&&... args) {
  auto pool = GetCpuThreadPool();
  // TODO: replace it with the code below when arrow 0.4.2 release
  // return pool->Submit(std::forward<Fn>(fn), std::forward<Args>(args)...);
  using PackagedTask = std::packaged_task<Result()>;
  auto task = PackagedTask(std::bind(std::forward<Fn>(fn), std::forward<Args>(args)...));
  auto fut = task.get_future();

  auto st = pool->Spawn(
      arrow::internal::detail::packaged_task_wrapper<Result>(std::move(task)));
  CHECK(st.ok()) << st.ToString();
  return fut;
}
}  // namespace utils
