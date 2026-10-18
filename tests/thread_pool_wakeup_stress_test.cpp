#include <async/coro_scope.hpp>
#include <async/thread_pool_executor/thread_pool_executor.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>
#include <utility>

namespace {
    constexpr size_t WorkerCount = 4;
    constexpr uint64_t Iterations = 2'000;

    class WakeupScope final : public shuttle::async::CoroScope {
    public:
        WakeupScope()
            : CoroScope{{.mode = shuttle::async::LaunchMode::Async,
                         .explicitExecutor = nullptr}} {}

        using CoroScope::launchAsync;

        void joinAll() {
            auto threadContextGuard = m_blockingExecutor.initializeThreadContext();
            registerJointerTaskHandle(std::noop_coroutine(), &m_blockingExecutor);
            if (checkCanDestroyed(std::noop_coroutine()) == nullptr) {
                m_blockingExecutor.waitForSignal();
            }
        }

        ~WakeupScope() {
            joinAll();
        }

    private:
        shuttle::async::BlockingExecutor m_blockingExecutor;
    };

    shuttle::async::AsyncTask<void> recordCompletion(
        shuttle::async::CoroScope,
        std::atomic<uint64_t>* completed)
    {
        completed->fetch_add(1, std::memory_order_relaxed);
        co_return;
    }

    bool waitUntilWorkerSleeps(
        const shuttle::async::ThreadPoolDiagnostics& diagnostics) noexcept
    {
        const auto deadline = std::chrono::steady_clock::now()
            + std::chrono::seconds{2};
        while (std::chrono::steady_clock::now() < deadline) {
            for (size_t worker = 0; worker < WorkerCount; ++worker) {
                const auto& counters = diagnostics.workers[worker];
                if (counters.idleWaits.load(std::memory_order_acquire)
                    > counters.waitReturns.load(std::memory_order_acquire)) {
                    return true;
                }
            }
            std::this_thread::yield();
        }
        return false;
    }
}

int main() {
    shuttle::async::ThreadPoolDiagnostics diagnostics;
    auto executorResult = shuttle::async::ThreadPoolExecutor::create(
        WorkerCount, nullptr, &diagnostics);
    if (!executorResult) {
        std::cerr << "Failed to create the worker pool\n";
        return 1;
    }

    auto& executor = *executorResult.value();
    WakeupScope scope;
    std::atomic<uint64_t> completed{0};

    for (uint64_t iteration = 0; iteration < Iterations; ++iteration) {
        if (!waitUntilWorkerSleeps(diagnostics)) {
            std::cerr << "No worker entered the idle wait before task "
                      << iteration << '\n';
            return 2;
        }

        auto task = scope.launchAsync(
            recordCompletion, static_cast<shuttle::async::IExecutor*>(&executor),
            &completed);
        if (!task.has_value()) {
            std::cerr << "Failed to submit wakeup task " << iteration << '\n';
            return 3;
        }
        shuttle::async::BlockingTask<void>{std::move(task)}.get();
        if (completed.load(std::memory_order_relaxed) != iteration + 1) {
            std::cerr << "Woken task did not complete exactly once\n";
            return 4;
        }
    }

    scope.joinAll();
    uint64_t incomingDequeues = 0;
    uint64_t idleWaits = 0;
    uint64_t waitReturns = 0;
    for (size_t worker = 0; worker < WorkerCount; ++worker) {
        const auto& counters = diagnostics.workers[worker];
        incomingDequeues += counters.incomingDequeues.load(std::memory_order_relaxed);
        idleWaits += counters.idleWaits.load(std::memory_order_relaxed);
        waitReturns += counters.waitReturns.load(std::memory_order_relaxed);
    }

    const uint64_t notifications =
        diagnostics.workNotifications.load(std::memory_order_relaxed);
    if (completed.load(std::memory_order_relaxed) != Iterations
        || incomingDequeues != Iterations
        || notifications == 0
        || waitReturns == 0
        || idleWaits < waitReturns) {
        std::cerr << "Wakeup stress counters do not match task completions\n";
        return 5;
    }

    std::cout << "Worker wakeup stress: " << completed.load()
              << " / " << Iterations << " tasks completed, "
              << notifications << " notifications, " << waitReturns
              << " wait returns\n";
    return 0;
}
