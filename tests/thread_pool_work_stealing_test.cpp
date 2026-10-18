#include <async/coro_scope.hpp>
#include <async/i_executor.hpp>
#include <async/thread_pool_executor/thread_pool_executor.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <optional>
#include <thread>

namespace {
    constexpr size_t MaxWorkerCount = 64;
    constexpr size_t Fanout = 8;
#if defined(NDEBUG)
    constexpr size_t TreeDepth = 6;
#else
    // Keep unoptimized MinGW coroutine transfers within the worker stack.
    constexpr size_t TreeDepth = 4;
#endif
    constexpr size_t WorkIterationsPerNode = 1024;
    constexpr uint64_t expectedNodeCount() noexcept {
        uint64_t total = 1;
        uint64_t levelSize = 1;
        for (size_t depth = 0; depth < TreeDepth; ++depth) {
            levelSize *= Fanout;
            total += levelSize;
        }
        return total;
    }
    constexpr uint64_t ExpectedNodeCount = expectedNodeCount();

    struct WorkStealingState {
        std::atomic<uint64_t> scheduled{0};
        std::atomic<uint64_t> completed{0};
        std::atomic<uint64_t> checksum{0};
        std::atomic<size_t> nextWorkerIndex{0};
        std::atomic<bool> failed{false};
        std::array<std::atomic<shuttle::async::IExecutor*>, MaxWorkerCount> workerExecutors{};
        std::array<std::atomic<uint64_t>, MaxWorkerCount> workerTaskCounts{};
    };

    void recordWorker(WorkStealingState& state) noexcept {
        auto* const executor = shuttle::async::IExecutor::getThreadPinnedExecutor();
        if (executor == nullptr) {
            state.failed.store(true, std::memory_order_relaxed);
            return;
        }

        const size_t workerCount = state.nextWorkerIndex.load(std::memory_order_acquire);
        for (size_t index = 0; index < workerCount; ++index) {
            if (state.workerExecutors[index].load(std::memory_order_acquire) == executor) {
                state.workerTaskCounts[index].fetch_add(1, std::memory_order_relaxed);
                return;
            }
        }

        const size_t index = state.nextWorkerIndex.fetch_add(1, std::memory_order_acq_rel);
        if (index >= MaxWorkerCount) {
            state.failed.store(true, std::memory_order_relaxed);
            return;
        }

        shuttle::async::IExecutor* expected = nullptr;
        if (!state.workerExecutors[index].compare_exchange_strong(
                expected, executor, std::memory_order_release, std::memory_order_relaxed)) {
            state.failed.store(true, std::memory_order_relaxed);
            return;
        }
        state.workerTaskCounts[index].fetch_add(1, std::memory_order_relaxed);
    }

    uint64_t doWork(uint64_t nodeId, size_t depth) noexcept {
        uint64_t value = nodeId + depth + 0x9e3779b97f4a7c15ULL;
        for (size_t iteration = 0; iteration < WorkIterationsPerNode; ++iteration) {
            value ^= value >> 12;
            value ^= value << 25;
            value ^= value >> 27;
            value *= 0x2545f4914f6cdd1dULL;
        }
        return value;
    }

    shuttle::async::AsyncTask<void> treeTask(
        shuttle::async::CoroScope scope,
        shuttle::async::ThreadPoolExecutor* executor,
        WorkStealingState* state,
        size_t depth,
        uint64_t nodeId)
    {
        state->scheduled.fetch_add(1, std::memory_order_relaxed);
        recordWorker(*state);

        std::array<std::optional<shuttle::async::AsyncTask<void>>, Fanout> children;
        size_t childCount = 0;
        if (depth > 0) {
            for (size_t childIndex = 0; childIndex < Fanout; ++childIndex) {
                const uint64_t childId = nodeId * Fanout + childIndex + 1;
                auto child = scope.launchAsync(
                    treeTask, static_cast<shuttle::async::IExecutor*>(executor),
                    executor, state, depth - 1, childId);
                if (!child.has_value()) {
                    state->failed.store(true, std::memory_order_relaxed);
                    continue;
                }
                children[childCount++].emplace(std::move(child));
            }
        }

        for (size_t childIndex = 0; childIndex < childCount; ++childIndex) {
            const auto result = co_await *children[childIndex];
            if (!result) {
                state->failed.store(true, std::memory_order_relaxed);
            }
        }

        state->checksum.fetch_xor(doWork(nodeId, depth), std::memory_order_relaxed);
        state->completed.fetch_add(1, std::memory_order_relaxed);
        co_return;
    }
}

int main() {
    const size_t workerCount = std::thread::hardware_concurrency();
    if (workerCount < 2 || workerCount > MaxWorkerCount) {
        std::cerr << "Work-stealing test requires 2 to " << MaxWorkerCount
                  << " reported hardware threads; detected " << workerCount << '\n';
        return 1;
    }

    auto executorResult = shuttle::async::ThreadPoolExecutor::create(workerCount, nullptr);
    if (!executorResult) {
        std::cerr << "Failed to create thread pool\n";
        return 1;
    }

    auto& executor = *executorResult.value();
    WorkStealingState state;
    shuttle::async::JoinableScope scope;
    auto root = scope.launchAsync(
        treeTask, static_cast<shuttle::async::IExecutor*>(&executor),
        &executor, &state, TreeDepth, uint64_t{0});
    if (!root.has_value()) {
        std::cerr << "Failed to launch tree root\n";
        return 1;
    }
    root.get();
    scope.joinAll();

    const uint64_t scheduled = state.scheduled.load(std::memory_order_acquire);
    const uint64_t completed = state.completed.load(std::memory_order_acquire);
    const size_t workersUsed = std::min(
        state.nextWorkerIndex.load(std::memory_order_acquire), MaxWorkerCount);
    uint64_t minTasksPerWorker = UINT64_MAX;
    uint64_t maxTasksPerWorker = 0;
    for (size_t index = 0; index < workersUsed; ++index) {
        const uint64_t tasks = state.workerTaskCounts[index].load(std::memory_order_relaxed);
        minTasksPerWorker = std::min(minTasksPerWorker, tasks);
        maxTasksPerWorker = std::max(maxTasksPerWorker, tasks);
    }

    std::cout << "AsyncTask work-stealing tree: " << completed << " / "
              << ExpectedNodeCount << " tasks completed, " << workersUsed << " / "
              << workerCount << " workers used, per-worker tasks " << minTasksPerWorker
              << "-" << maxTasksPerWorker << '\n';

    if (state.failed.load(std::memory_order_relaxed)
        || scheduled != ExpectedNodeCount
        || completed != ExpectedNodeCount
        || workersUsed < 2
        || state.checksum.load(std::memory_order_relaxed) == 0) {
        std::cerr << "AsyncTask work-stealing stress test failed\n";
        return 1;
    }

    std::cout << "AsyncTask work-stealing stress test passed\n";
    return 0;
}
