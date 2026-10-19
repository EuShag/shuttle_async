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
    constexpr uint64_t Iterations = 256;

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

}

int main() {
    auto executorResult = shuttle::async::ThreadPoolExecutor::create(
        WorkerCount, nullptr);
    if (!executorResult) {
        std::cerr << "Failed to create the worker pool\n";
        return 1;
    }

    auto& executor = *executorResult.value();
    WakeupScope scope;
    std::atomic<uint64_t> completed{0};

    for (uint64_t iteration = 0; iteration < Iterations; ++iteration) {
        std::this_thread::sleep_for(std::chrono::milliseconds{1});

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

    const uint64_t completionCount = completed.load(std::memory_order_relaxed);
    if (completionCount != Iterations) {
        std::cerr << "Wakeup stress completed " << completionCount << " / "
                  << Iterations << " tasks\n";
        return 5;
    }

    std::cout << "Worker wakeup stress: " << completionCount
              << " / " << Iterations << " tasks completed\n";
    return 0;
}
