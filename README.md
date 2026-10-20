# shuttle_async

**shuttle_async** is an experimental, high-performance C++23 coroutine task and execution library designed by **Eugenii Shagunov** as the computing foundation for the **Shuttle Engine** (Vulkan API). It provides low-latency coroutine tasks, decentralized lock-free executors, structured child-task scopes, cooperative cancellation, and allocator-aware shared state.

The project is optimized for extreme core utilization on modern hardware while enforcing strict memory constraints on the critical frame path.

## Features

*   **`AsyncTask<T>` & `BlockingTask<T>`:** High-performance asynchronous tasks backed by symmetric transfer, and explicit synchronous boundaries via `get()`.
*   **`ThreadPoolExecutor`:** A fully decentralized multi-threaded scheduler using per-worker Chase-Lev deques, a global `moodycamel::ConcurrentQueue` funnel, randomized work-stealing, and atomic coalesced worker wake-ups (`Packed sleep state`) to mitigate the Thundering Herd problem.
*   **`OneThreadExecutor`:** Serial asynchronous executor pinned to a dedicated execution thread.
*   **Structured Concurrency:** Strict tree-like lifetime management via `CoroScope` and `JoinableScope` (root scope) combined with the internal `joinTask` guard mechanism to prevent dangling references and memory corruption of parent contexts.
*   **Lock-Free State Machine:** Move-only `AsyncPromise<T>` and `AsyncFuture<T>` endpoints coordinated by a reference-counted, lock-free `AsyncSharedState<T>`.
*   **Allocator-Aware Runtime:** Custom allocator hooks allowing precise memory placement. All core library allocations and coroutine frames are explicitly backed by the **mimalloc** low-latency memory allocator.
*   **Robust Architecture:** `std::expected`-based system error propagation and cooperative cancellation tracking via tokens.

*Note: Timers and asynchronous I/O are currently not implemented. **Win32 asynchronous I/O (IOCP / Overlapped operations) is planned work** for the upcoming milestone.*

> ⚠️ **Critical Safety Warning:** The `LaunchMode::Immediate` configuration executes a child coroutine synchronously on the calling thread until its first actual suspension point. Inside this mode, using `co_await` is permitted **strictly within the same thread context (e.g., in conjunction with a `ThreadPinnedExecutor` or `OneThreadExecutor`)**. Thread migration to alternative pools or workers inside an Immediate task violates Structured Concurrency invariants and **will lead to undefined behavior (UB)**.

## Requirements

*   A C++23 compliant toolchain (e.g., GCC 13+, Clang 16+, MSVC 19.36+) with full coroutine and `std::expected` support.
*   CMake 3.21 or newer.
*   Initialized Git submodules (`external/mimalloc` and `external/concurrentqueue`).

The runtime is actively developed and validated on Windows via MinGW and CLion. Cross-platform validation for other compiler and OS configurations is a work in progress.

## Build and Test

To fetch dependencies, build the core targets, and execute the test suite from a shell with a C++23 toolchain on your `PATH`:

```powershell
git submodule update --init --recursive
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DASYNC_BUILD_TESTS=ON -DASYNC_BUILD_EXAMPLES=ON
cmake --build build --config Release
ctest --test-dir build -C Release --output-on-failure
```

*   **Debug Configuration:** Use a separate build directory for Debug configurations. For GCC Debug builds, the CMake target automatically appends `-foptimize-sibling-calls` to prevent stack overflows during coroutine symmetric transfers.
*   **Execution Note:** The comprehensive scaling stress test heavily saturates the CPU and may take several minutes to run.
*   **CMake Export:** The standalone library export target is `async::async`, which exposes the `include/` headers and enforces the C++23 standard constraint.

## Examples

*   [`examples/thread_pool_tasks.cpp`](examples/thread_pool_tasks.cpp) — Spawns an `AsyncTask` onto the decentralized thread pool and handles the asynchronous result.
*   [`examples/shared_state_custom_allocator.cpp`](examples/shared_state_custom_allocator.cpp) — Demonstrates state allocation using a custom allocator along with raw promise/future endpoint moves.

Compile the examples directly via CMake:

```powershell
cmake --build build --config Release --target async_thread_pool_example async_shared_state_example
```

*Guidelines:* `JoinableScope` blocks and waits for child tasks upon destruction or joining. Do not let coroutines capture references to stack objects whose lifetimes end before the join boundary. Avoid invoking `BlockingTask::get()` from a pool worker thread if that same worker is required to advance the task's progress.

## Architecture and Ownership

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

`AsyncSharedState` is never instantiated as a user-owned stack variable. The internal factory dynamically manages allocations through `mimalloc` and returns precise endpoints. Custom allocators can safely manage the state block's memory placement without invalidating coroutine frame locations.

## Benchmarks and Performance

A large-scale stress test of the scheduler was conducted on an **Intel Core i9-14900KF** processor (24 cores, 32 threads) under a **Release configuration with maximum optimization (-O3)**. All logging and diagnostic outputs were completely stripped from the code to measure the pure performance of the lock-free algorithms.

### Workload Profiles:
1.  **"Small tasks" Profile:** 50,000 atomic tasks, 3 iterations. Designed to test the nanosecond coordination overhead and cache bus throughput.
2.  **"Large tasks" Profile:** 5,000 heavy tasks, 3 iterations. Designed to test pure core compute utilization with minimal queue interaction.
3.  **"Uneven tasks" Profile:** 10,000 tasks, 3 iterations. A mix of 10% heavy and 90% light tasks, emulating the chaotic workload of a real game frame.

### Final Release Benchmark Results

| Workers | Small Tasks (50k) | Speedup | Large Tasks (5k) | Speedup | Uneven Tasks (10k) | Speedup |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | 0.889804 s | **1.00x** | 32.8135 s | **1.00x** | 13.6614 s | **1.00x** |
| **2** | 0.442065 s | **2.01x** | 15.9649 s | **2.06x** | 6.73722 s | **2.03x** |
| **4** | 0.202313 s | **4.40x** | 8.22157 s | **3.99x** | 3.37063 s | **4.05x** |
| **8** | 0.105425 s | **8.44x** | 4.18599 s | **7.84x** | 1.73479 s | **7.87x** |
| **16** | 0.061053 s | **14.57x** | 2.36845 s | **13.85x** | 1.00655 s | **13.57x** |
| **31** | 0.033878 s | **26.26x** | 1.29606 s | **25.32x** | 0.553842 s | **24.67x** |
| **32** | 0.033976 s | **26.19x** | 1.26743 s | **25.89x** | 0.540964 s | **25.25x** |

*Parallel efficiency summary table (All workload profiles). Median execution time in seconds and speedup factor relative to single-threaded runtime.*

## API Reference Documentation

The interactive C++ documentation is automatically generated and deployed via a GitHub Actions CI/CD pipeline upon every push to the mainline branch:

👉 **Online API Reference:** [`https://github.io`](https://eushag.github.io/shuttle_async/)

Architectural PlantUML source graphs mapping components, sequences, and state machines are located in the [`docs/uml/`](docs/uml/) directory.

## License

This project is licensed under the terms of the MIT License. See the [`LICENSE`](LICENSE) file for details. Third-party vendor software located in `external/` maintains separate licensing terms as provided by their respective authors.
