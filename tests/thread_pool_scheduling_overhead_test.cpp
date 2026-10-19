#include <async/coro_scope.hpp>
#include <async/i_executor.hpp>
#include <async/mi_allocator.hpp>
#include <async/thread_pool_executor/thread_pool_executor.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

namespace {
    constexpr uint64_t TaskCount = 20'000;
    constexpr size_t MeasurementRuns = 3;
    constexpr std::array<size_t, 6> WorkerCounts{1, 2, 4, 8, 16, 32};
    constexpr std::array<size_t, 2> WorkProfiles{0, 2048};
    constexpr std::array<size_t, 3> BatchSizes{32, 512, 4096};
    constexpr size_t MaxWorkers = 32;

    struct alignas(64) WorkerStats {
        shuttle::async::IExecutor* executor{nullptr};
        uint64_t tasks{0};
        uint64_t stolenTasks{0};
    };

    struct State {
        explicit State(size_t iterations, size_t batch, bool collectMetrics) noexcept
            : workIterations(iterations),
              batchSize(batch),
              collectMetrics(collectMetrics),
              generation(nextGeneration.fetch_add(1, std::memory_order_relaxed)) {}

        const size_t workIterations;
        const size_t batchSize;
        const bool collectMetrics;
        const uint64_t generation;
        std::atomic<uint64_t> taskAllocations{0};
        std::atomic<uint64_t> taskAllocatedBytes{0};
        std::atomic<size_t> nextWorker{0};
        std::atomic<bool> failed{false};
        std::atomic<shuttle::async::IExecutor*> rootExecutor{nullptr};
        std::array<WorkerStats, MaxWorkers> workers{};

        inline static std::atomic<uint64_t> nextGeneration{1};
    };

    struct WorkerCache {
        const State* state{nullptr};
        uint64_t generation{0};
        size_t index{0};
    };

    class BenchmarkScope final : public shuttle::async::CoroScope {
    public:
        BenchmarkScope()
            : CoroScope{{.mode = shuttle::async::LaunchMode::Async,
                         .explicitExecutor = nullptr}} {}

        void joinAll() {
            auto threadContextGuard = m_blockingExecutor.initializeThreadContext();
            registerJointerTaskHandle(std::noop_coroutine(), &m_blockingExecutor);
            if (checkCanDestroyed(std::noop_coroutine()) == nullptr) {
                m_blockingExecutor.waitForSignal();
            }
        }

        ~BenchmarkScope() {
            joinAll();
        }

    private:
        shuttle::async::BlockingExecutor m_blockingExecutor;
    };

    WorkerCache& currentWorkerCache() noexcept {
        static thread_local WorkerCache cache;
        return cache;
    }

    WorkerStats* recordCurrentWorker(State& state) noexcept {
        auto* const executor = shuttle::async::IExecutor::getThreadPinnedExecutor();
        if (executor == nullptr) {
            state.failed.store(true, std::memory_order_relaxed);
            return nullptr;
        }

        WorkerCache& cache = currentWorkerCache();
        if (cache.state != &state || cache.generation != state.generation) {
            const size_t index = state.nextWorker.fetch_add(1, std::memory_order_relaxed);
            if (index >= state.workers.size()) {
                state.failed.store(true, std::memory_order_relaxed);
                return nullptr;
            }

            state.workers[index].executor = executor;
            cache = WorkerCache{&state, state.generation, index};
        }

        WorkerStats& stats = state.workers[cache.index];
        if (stats.executor != executor) {
            state.failed.store(true, std::memory_order_relaxed);
            return nullptr;
        }

        ++stats.tasks;
        const auto* const root = state.rootExecutor.load(std::memory_order_acquire);
        if (root != nullptr && root != executor) {
            ++stats.stolenTasks;
        }
        return &stats;
    }

    uint64_t doWork(uint64_t taskId, size_t iterations) noexcept {
        uint64_t value = taskId + 0x9e3779b97f4a7c15ULL;
        for (size_t i = 0; i < iterations; ++i) {
            value ^= value >> 12;
            value ^= value << 25;
            value ^= value >> 27;
            value *= 0x2545f4914f6cdd1dULL;
        }
        return value;
    }

    shuttle::async::AsyncTask<void> leafTask(
        shuttle::async::CoroScope,
        State* state,
        uint64_t* result,
        uint64_t taskId)
    {
        if (state->collectMetrics && recordCurrentWorker(*state) == nullptr) {
            co_return;
        }
        result[taskId] = doWork(taskId, state->workIterations);
        co_return;
    }

    shuttle::async::AsyncTask<void> localProducerTask(
        shuttle::async::CoroScope scope,
        shuttle::async::IExecutor* executor,
        State* state,
        uint64_t* results)
    {
        if (state->collectMetrics) {
            auto* const root = shuttle::async::IExecutor::getThreadPinnedExecutor();
            if (root == nullptr) {
                state->failed.store(true, std::memory_order_relaxed);
                co_return;
            }
            state->rootExecutor.store(root, std::memory_order_release);
        }

        std::vector<shuttle::async::AsyncTask<void>> tasks;
        tasks.reserve(state->batchSize);
        for (uint64_t first = 0; first < TaskCount; first += state->batchSize) {
            const size_t count = static_cast<size_t>(
                std::min<uint64_t>(state->batchSize, TaskCount - first));
            auto submitBatch = [&] {
                for (size_t offset = 0; offset < count; ++offset) {
                    auto task = scope.launchAsync(
                        leafTask, executor, state, results, first + offset);
                    if (!task.has_value()) {
                        return false;
                    }
                    tasks.emplace_back(std::move(task));
                }
                return true;
            };
            shuttle::async::MiAllocationStats batchStats;
            bool submitted = false;
            if (state->collectMetrics) {
                {
                    shuttle::async::MiAllocatorBase::AllocationCounterScope allocationCounter{
                        batchStats};
                    submitted = submitBatch();
                }
                state->taskAllocations.fetch_add(
                    batchStats.allocations, std::memory_order_relaxed);
                state->taskAllocatedBytes.fetch_add(
                    batchStats.bytes, std::memory_order_relaxed);
            } else {
                submitted = submitBatch();
            }
            if (!submitted) {
                state->failed.store(true, std::memory_order_relaxed);
                co_return;
            }

            for (auto& task : tasks) {
                if ((co_await task).isCancelled) {
                    state->failed.store(true, std::memory_order_relaxed);
                }
            }
            tasks.clear();
        }
        co_return;
    }

    bool runOnce(size_t workerCount, size_t workIterations, size_t batchSize,
                 bool localProducer,
                 double& elapsedSeconds, uint64_t& stolenTasks,
                 size_t& workersUsed,
                 uint64_t& allocationCount,
                 uint64_t& allocatedBytes,
                 bool collectMetrics) {
        auto executorResult = shuttle::async::ThreadPoolExecutor::create(
            workerCount, nullptr);
        if (!executorResult) {
            std::cerr << "Failed to create thread pool with " << workerCount << " workers\n";
            return false;
        }

        auto& executor = *executorResult.value();
        State state{workIterations, batchSize, collectMetrics};
        std::vector<uint64_t> results(TaskCount, 0);
        BenchmarkScope scope;
        const auto start = std::chrono::steady_clock::now();

        if (localProducer) {
            auto root = [&] {
                if (collectMetrics) {
                    shuttle::async::MiAllocationStats rootStats;
                    shuttle::async::MiAllocatorBase::AllocationCounterScope allocationCounter{
                        rootStats};
                    auto task = scope.launchAsync(
                        localProducerTask, static_cast<shuttle::async::IExecutor*>(&executor),
                        static_cast<shuttle::async::IExecutor*>(&executor),
                        &state, results.data());
                    allocationCount += rootStats.allocations;
                    allocatedBytes += rootStats.bytes;
                    return task;
                }
                return scope.launchAsync(
                    localProducerTask, static_cast<shuttle::async::IExecutor*>(&executor),
                    static_cast<shuttle::async::IExecutor*>(&executor),
                    &state, results.data());
            }();
            if (!root.has_value()) {
                std::cerr << "Failed to launch local producer task\n";
                return false;
            }
            shuttle::async::BlockingTask<void>{std::move(root)}.get();
            if (state.failed.load(std::memory_order_relaxed)) {
                std::cerr << "Local producer task failed\n";
                return false;
            }
        } else {
            std::vector<shuttle::async::AsyncTask<void>> tasks;
            tasks.reserve(TaskCount);
            auto submitAllTasks = [&] {
                for (uint64_t taskId = 0; taskId < TaskCount; ++taskId) {
                    auto task = scope.launchAsync(
                        leafTask, static_cast<shuttle::async::IExecutor*>(&executor),
                        &state, results.data(), taskId);
                    if (!task.has_value()) {
                        std::cerr << "Failed to launch task " << taskId << '\n';
                        return false;
                    }
                    tasks.emplace_back(std::move(task));
                }
                return true;
            };
            if (collectMetrics) {
                shuttle::async::MiAllocationStats launchStats;
                shuttle::async::MiAllocatorBase::AllocationCounterScope allocationCounter{
                    launchStats};
                if (!submitAllTasks()) {
                    return false;
                }
                allocationCount += launchStats.allocations;
                allocatedBytes += launchStats.bytes;
            } else if (!submitAllTasks()) {
                return false;
            }
        }

        scope.joinAll();
        elapsedSeconds = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - start).count();
        if (collectMetrics) {
            allocationCount += state.taskAllocations.load(std::memory_order_relaxed);
            allocatedBytes += state.taskAllocatedBytes.load(std::memory_order_relaxed);
        }

        for (uint64_t const result : results) {
            if (result == 0) {
                std::cerr << "A task did not produce a result\n";
                return false;
            }
        }

        uint64_t completedTasks = TaskCount;
        if (collectMetrics) {
            workersUsed = std::min(state.nextWorker.load(std::memory_order_relaxed), MaxWorkers);
            stolenTasks = 0;
            completedTasks = 0;
            for (size_t i = 0; i < workersUsed; ++i) {
                stolenTasks += state.workers[i].stolenTasks;
                completedTasks += state.workers[i].tasks;
            }
        } else {
            workersUsed = 0;
            stolenTasks = 0;
        }
        return !state.failed.load(std::memory_order_relaxed)
            && completedTasks == TaskCount;
    }

    void runScenario(size_t workerCount, size_t workIterations, size_t batchSize,
                     bool localProducer) {
        std::array<double, MeasurementRuns> elapsed{};
        for (size_t run = 0; run < MeasurementRuns; ++run) {
            uint64_t ignoredSteals = 0;
            size_t ignoredWorkers = 0;
            uint64_t ignoredAllocations = 0;
            uint64_t ignoredBytes = 0;
            if (!runOnce(workerCount, workIterations, batchSize, localProducer,
                    elapsed[run], ignoredSteals, ignoredWorkers,
                    ignoredAllocations, ignoredBytes, false)) {
                std::exit(1);
            }
        }
        size_t medianIndex = 0;
        for (size_t i = 1; i < MeasurementRuns; ++i) {
            if (elapsed[i] < elapsed[medianIndex]) {
                medianIndex = i;
            }
        }

        uint64_t stolenTasks = 0;
        size_t workersUsed = 0;
        uint64_t allocationCount = 0;
        uint64_t allocatedBytes = 0;
        double metricsRunSeconds = 0;
        if (!runOnce(workerCount, workIterations, batchSize, localProducer,
                metricsRunSeconds, stolenTasks, workersUsed,
                allocationCount, allocatedBytes, true)) {
            std::exit(1);
        }
        const uint64_t expectedAllocations = TaskCount * 2 + (localProducer ? 2 : 0);
        if (allocationCount != expectedAllocations || allocatedBytes == 0) {
            std::cerr << "Unexpected task allocation profile: "
                      << allocationCount << " allocations, " << allocatedBytes
                      << " bytes\n";
            std::exit(1);
        }

        const double median = elapsed[medianIndex];
        const uint64_t taskBatchSize = batchSize == 0 ? TaskCount : batchSize;
        const double estimatedBatchResidentBytes =
            static_cast<double>(allocatedBytes) * taskBatchSize / TaskCount
            + static_cast<double>(taskBatchSize)
                * sizeof(shuttle::async::AsyncTask<void>);
        std::cout << (localProducer ? "local" : "global") << '\t'
                  << workIterations << '\t' << workerCount << '\t'
                  << median << '\t'
                  << static_cast<uint64_t>(TaskCount / median) << '\t'
                  << stolenTasks << '\t' << workersUsed << '\t'
                  << allocationCount << '\t'
                  << static_cast<double>(allocationCount) / TaskCount
                  << '\t' << allocatedBytes << '\t'
                  << static_cast<double>(allocatedBytes) / TaskCount
                  << '\t' << estimatedBatchResidentBytes
                  << '\t' << batchSize << '\n';
    }
}

int main() {
    struct Scenario {
        size_t iterations;
        size_t workers;
        size_t batchSize;
        bool localProducer;
    };
    std::vector<Scenario> scenarios;
    for (const size_t workIterations : WorkProfiles) {
        for (const size_t workerCount : WorkerCounts) {
            scenarios.push_back({workIterations, workerCount, 0, false});
            for (const size_t batchSize : BatchSizes) {
                scenarios.push_back({workIterations, workerCount, batchSize, true});
            }
        }
    }
    std::mt19937_64 random{0x7d0a4e9351ULL};
    std::shuffle(scenarios.begin(), scenarios.end(), random);

    std::cout << "# timing: 3 runs; task and allocator metrics: separate instrumented run\n";
    std::cout << "mode\titerations\tworkers\tmedian_seconds\ttasks_per_second"
                 "\tstolen_tasks\tworkers_used\tmimalloc_allocations\tallocations_per_task"
                 "\ttask_allocation_bytes\tbytes_per_task"
                 "\testimated_batch_resident_bytes\tbatch_size\n";
    for (const Scenario& scenario : scenarios) {
        runScenario(scenario.workers, scenario.iterations,
            scenario.batchSize, scenario.localProducer);
    }
    return 0;
}
