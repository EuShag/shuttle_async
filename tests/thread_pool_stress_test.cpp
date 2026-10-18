#include <async/coro_scope.hpp>
#include <async/thread_pool_executor/thread_pool_executor.hpp>

#include <array>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <initializer_list>
#include <thread>
#include <vector>

namespace {
    constexpr size_t MaxWorkerCount = 32;
    constexpr size_t MeasurementRuns = 3;

    struct WorkloadProfile {
        char const* name;
        uint64_t taskCount;
        size_t uniformIterations;
        size_t lightIterations;
        size_t heavyIterations;
        uint64_t heavyTaskInterval;
    };

    constexpr std::array Workloads{
        WorkloadProfile{"Small tasks", 50'000, 10'000, 0, 0, 0},
        WorkloadProfile{"Large tasks", 5'000, 4'000'000, 0, 0, 0},
        WorkloadProfile{"Uneven tasks", 10'000, 0, 50'000, 8'000'000, 10}
    };

    struct StartGate {
        std::atomic<size_t> nextWorker{0};
        std::atomic<size_t> readyWorkers{0};
        std::atomic<bool> open{false};
        std::array<std::thread::id, MaxWorkerCount> workerIds{};
    };

    class StressScope final : public shuttle::async::CoroScope {
    public:
        StressScope()
            : CoroScope{{.mode = shuttle::async::LaunchMode::Async, .explicitExecutor = nullptr}} {}

        using CoroScope::launchAsync;

        void joinAll() {
            auto threadContextGuard = m_blockingExecutor.initializeThreadContext();
            registerJointerTaskHandle(std::noop_coroutine(), &m_blockingExecutor);
            if (checkCanDestroyed(std::noop_coroutine()) == nullptr) {
                m_blockingExecutor.waitForSignal();
            }
        }

        ~StressScope() {
            joinAll();
        }

    private:
        shuttle::async::BlockingExecutor m_blockingExecutor;
    };

    shuttle::async::AsyncTask<void> stressTask(
        shuttle::async::CoroScope,
        StartGate* startGate,
        uint64_t& result,
        uint64_t taskId,
        WorkloadProfile const& workload)
    {
        if (startGate != nullptr) {
            const size_t workerIndex = startGate->nextWorker.fetch_add(1, std::memory_order_relaxed);
            if (workerIndex >= startGate->workerIds.size()) {
                std::terminate();
            }
            startGate->workerIds[workerIndex] = std::this_thread::get_id();
            startGate->readyWorkers.fetch_add(1, std::memory_order_release);
            startGate->readyWorkers.notify_all();

            while (!startGate->open.load(std::memory_order_acquire)) {
                startGate->open.wait(false, std::memory_order_acquire);
            }
        }

        uint64_t value = taskId + 0x9e3779b97f4a7c15ULL;
        size_t const workIterations = workload.uniformIterations != 0
            ? workload.uniformIterations
            : (taskId % workload.heavyTaskInterval == 0
                ? workload.heavyIterations
                : workload.lightIterations);
        for (size_t iteration = 0; iteration < workIterations; ++iteration) {
            value ^= value >> 12;
            value ^= value << 25;
            value ^= value >> 27;
            value *= 0x2545f4914f6cdd1dULL;
        }

        result = value;
        co_return;
    }

    bool launchTask(
        StressScope& scope,
        shuttle::async::ThreadPoolExecutor& executor,
        std::vector<shuttle::async::AsyncTask<void>>& tasks,
        std::vector<uint64_t>& results,
        StartGate* startGate,
        uint64_t taskId,
        WorkloadProfile const& workload)
    {
        auto task = scope.launchAsync(
            stressTask, &executor, startGate, results[taskId], taskId, workload);
        if (!task.has_value()) {
            return false;
        }

        tasks.emplace_back(std::move(task));
        return true;
    }

    bool runStressTest(
        size_t workerCount,
        WorkloadProfile const& workload,
        double& elapsedSeconds)
    {
        auto executorResult = shuttle::async::ThreadPoolExecutor::create(workerCount, nullptr);
        if (!executorResult) {
            std::cerr << "Failed to create thread pool with " << workerCount << " workers\n";
            return false;
        }

        auto& executor = *executorResult.value();
        StressScope scope;
        StartGate startGate;
        std::vector<uint64_t> results(workload.taskCount);
        std::vector<shuttle::async::AsyncTask<void>> tasks;
        tasks.reserve(workload.taskCount);

        const auto start = std::chrono::steady_clock::now();
        for (uint64_t taskId = 0; taskId < workerCount; ++taskId) {
            if (!launchTask(scope, executor, tasks, results, &startGate, taskId, workload)) {
                startGate.open.store(true, std::memory_order_release);
                startGate.open.notify_all();
                for (auto& task : tasks) {
                    static_cast<void>(task.get(&executor));
                }
                scope.joinAll();
                std::cerr << "Failed to launch startup task\n";
                return false;
            }
        }

        size_t readyWorkers = startGate.readyWorkers.load(std::memory_order_acquire);
        while (readyWorkers < workerCount) {
            startGate.readyWorkers.wait(readyWorkers, std::memory_order_acquire);
            readyWorkers = startGate.readyWorkers.load(std::memory_order_acquire);
        }

        for (size_t first = 0; first < workerCount; ++first) {
            for (size_t second = first + 1; second < workerCount; ++second) {
                if (startGate.workerIds[first] == startGate.workerIds[second]) {
                    startGate.open.store(true, std::memory_order_release);
                    startGate.open.notify_all();
                    for (auto& task : tasks) {
                        static_cast<void>(task.get(&executor));
                    }
                    scope.joinAll();
                    std::cerr << "Startup tasks did not occupy distinct workers\n";
                    return false;
                }
            }
        }

        startGate.open.store(true, std::memory_order_release);
        startGate.open.notify_all();

        for (uint64_t taskId = workerCount; taskId < workload.taskCount; ++taskId) {
            if (!launchTask(scope, executor, tasks, results, nullptr, taskId, workload)) {
                for (auto& task : tasks) {
                    static_cast<void>(task.get(&executor));
                }
                scope.joinAll();
                std::cerr << "Failed to launch stress task " << taskId << '\n';
                return false;
            }
        }

        for (auto& task : tasks) {
            static_cast<void>(task.get(&executor));
        }
        scope.joinAll();
        elapsedSeconds = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - start).count();

        for (uint64_t const result : results) {
            if (result == 0) {
                std::cerr << "A stress task did not produce a result\n";
                return false;
            }
        }
        return true;
    }
}

int main() {
    for (WorkloadProfile const& workload : Workloads) {
        std::cout << workload.name << " (" << workload.taskCount
                  << " tasks, " << MeasurementRuns << " runs)\n";
        std::cout << "Workers  Median (s)  Min (s)  Max (s)  Tasks/s\n";
        for (size_t workerCount = 1; workerCount <= MaxWorkerCount; ++workerCount) {
            std::array<double, MeasurementRuns> elapsedRuns{};
            for (double& elapsedSeconds : elapsedRuns) {
                if (!runStressTest(workerCount, workload, elapsedSeconds)) {
                    return 1;
                }
            }
            std::sort(elapsedRuns.begin(), elapsedRuns.end());
            const double medianSeconds = elapsedRuns[MeasurementRuns / 2];
            std::cout << workerCount << "\t  " << medianSeconds << "\t  "
                      << elapsedRuns.front() << "\t  " << elapsedRuns.back()
                      << "\t  "
                      << static_cast<uint64_t>(workload.taskCount / medianSeconds)
                      << '\n';
        }
    }
    std::cout << "Thread pool stress test passed\n";
    return 0;
}
