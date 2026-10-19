# shuttle_async

**shuttle_async** is an experimental C++23 coroutine task and execution library by **Eugenii Shagunov**. It provides coroutine tasks, asynchronous and single-thread executors, structured child-task scopes, cooperative cancellation, and dynamically allocated shared task state.

The project is under active development and is not yet a production-hardened runtime. The tests and measurements below describe what has been exercised; they do not prove universal correctness or performance.

## Features

- `AsyncTask<T>` for asynchronous coroutine work and `BlockingTask<T>` for an explicit synchronous boundary.
- `ThreadPoolExecutor` with per-worker Chase-Lev deques, a global incoming queue, randomized work stealing, and coalesced worker wake-ups.
- `OneThreadExecutor` for serial asynchronous execution on a dedicated thread.
- `CoroScope` and `JoinableScope` for child-task lifetime management.
- Move-only `AsyncPromise<T>` and `AsyncFuture<T>` backed by dynamically allocated `AsyncSharedState<T>`.
- A custom allocator hook for shared-state placement and mimalloc-backed library allocations.
- `std::expected`-based operation errors and cooperative task cancellation.

Timers and asynchronous I/O are not implemented. **Win32 asynchronous I/O is planned work**, not an existing feature.

> ⚠️ **Critical Safety Warning:** The `launchImmediate` method executes a coroutine synchronously on the calling thread until its first suspension point. Inside this mode, using `co_await` is allowed **strictly within the same thread context (e.g., in conjunction with a ThreadPinnedExecutor / OneThreadExecutor)**. Thread migration to other threads or pools inside an Immediate task is strictly prohibited. Changing the execution context to a different thread will break Structured Concurrency invariants and **lead to undefined behavior (UB)**.

## Requirements

- A C++23 compiler with coroutine and `std::expected` support.
- CMake 3.21 or newer.
- Initialized Git submodules (mimalloc and concurrentqueue).

The library is currently developed and exercised on Windows with MinGW/CLion. Other compiler and operating-system combinations need additional validation.

## Build and test

From PowerShell or another shell with CMake and a C++23 toolchain on `PATH`:

```powershell
git submodule update --init --recursive
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DASYNC_BUILD_TESTS=ON -DASYNC_BUILD_EXAMPLES=ON
cmake --build build --config Release
ctest --test-dir build -C Release --output-on-failure
```

Use a separate build directory for Debug. With a multi-configuration generator, select the configuration using `--config Debug` or `--config Release`. GCC Debug builds require `-foptimize-sibling-calls` to prevent stack growth during coroutine symmetric transfer; the CMake target adds this flag automatically. The full scaling stress test can take several minutes and heavily load the CPU.

The standalone CMake build uses the vendored `external/` dependencies. `async::async` is the library target and exports the `include/` directory and C++23 requirement.

## Examples

- [`examples/thread_pool_tasks.cpp`](examples/thread_pool_tasks.cpp) launches an `AsyncTask` on the thread pool and waits for its result.
- [`examples/shared_state_custom_allocator.cpp`](examples/shared_state_custom_allocator.cpp) creates shared state with a custom allocator and uses its move-only promise/future endpoints.

Build them with:

```powershell
cmake --build build --config Release --target async_thread_pool_example async_shared_state_example
```

`JoinableScope` waits for child work when joined or destroyed. Do not let a coroutine retain references to objects whose lifetimes end before that join. `BlockingTask::get()` blocks the calling OS thread; avoid calling it from a worker when the task requires that same worker to make progress.

## Architecture and ownership

```text
AsyncTask<T> / BlockingTask<T>
       | completion
       v
AsyncPromise<T> <-> AsyncSharedState<T> <-> AsyncFuture<T>
       | scheduling                         | waiter continuation
       +------------------- IExecutor <-----+
                               |
                    +----------+----------+
                    v                     v
           ThreadPoolExecutor      OneThreadExecutor
                    |
        local Chase-Lev queues + global incoming queue
        randomized work stealing + coalesced worker wake-ups
```

`AsyncSharedState` is not constructed as a user-owned stack object. `makeAsyncState` dynamically creates its allocation and returns move-only promise/future endpoints. A caller-provided allocator can control how the shared-state block is allocated.

## Benchmarks and Performance

A large-scale stress test of the scheduler was conducted on an **Intel Core i9-14900KF** processor (24 cores, 32 threads) under a **Release configuration with maximum optimization (-O3)**. All logging and diagnostic outputs were completely stripped from the code to measure the pure performance of the lock-free algorithms.

### Workload Profiles:
1. **"Small tasks" Profile:** 50,000 atomic tasks, 3 iterations. Designed to test the nanosecond coordination overhead and cache bus throughput.
2. **"Large tasks" Profile:** 5,000 heavy tasks, 3 iterations. Designed to test pure core compute utilization with minimal queue interaction.
3. **"Uneven tasks" Profile:** 10,000 tasks, 3 iterations. A mix of 10% heavy and 90% light tasks, emulating the chaotic workload of a real game frame.

### Final Release Benchmark Results

| Workers | Small Tasks (50k) | Speedup | Large Tasks (5k) | Speedup | Uneven Tasks (10k) | Speedup |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | 0.889804 s | 1.00x | 32.8135 s | 1.00x | 13.6614 s | 1.00x |
| **2** | 0.442065 s | 2.01x | 15.9649 s | 2.06x | 6.73722 s | 2.03x |
| **4** | 0.202313 s | 4.40x | 8.22157 s | 3.99x | 3.37063 s | 4.05x |
| **8** | 0.105425 s | 8.44x | 4.18599 s | 7.84x | 1.73479 s | 7.87x |
| **16** | 0.061053 s | 14.57x | 2.36845 s | 13.85x | 1.00655 s | 13.57x |
| **31** | 0.033878 s | 26.26x | 1.29606 s | 25.32x | 0.553842 s | 24.67x |
| **32** | 0.033976 s | 26.19x | 1.26743 s | 25.89x | 0.540964 s | 25.25x |

*Parallel efficiency summary table (All workload profiles). Median execution time in seconds and speedup factor relative to single-threaded runtime.*


## Roadmap

- Continue validating task lifetime, executor shutdown, wake-up races, and failure paths under stress.
- Improve reproducible benchmarks and verify scheduler changes across workload shapes and toolchains.
- **Design and implement asynchronous I/O for Win32**, including cancellation, completion delivery, and integration with the executor API. This is planned; no Win32 I/O API is available yet.

## Documentation

- English PlantUML diagrams: `uml/en/README.md`
- Russian PlantUML diagrams: `uml/ru/README.md`
- [English Doxygen API reference](Doxyfile)
- Generate HTML with `doxygen Doxyfile`; output: `docs/api/en/html/`.
- Generate all diagram images with `cmake --build build --target uml_png`; PNGs are written to `build/uml/en/` and `build/uml/ru/`. Install Java and PlantUML first; see the language-specific diagram indexes.

## License

MIT; see [`LICENSE`](LICENSE). Third-party dependencies retain their own licenses in `external/`.
