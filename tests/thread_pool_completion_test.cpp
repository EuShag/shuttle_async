#include <async/coro_scope.hpp>
#include <async/thread_pool_executor/thread_pool_executor.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <utility>
#include <vector>

namespace {
    constexpr size_t TaskCount = 100'000;
    constexpr auto CompletionTimeout = std::chrono::seconds{10};

    class CompletionScope final : public shuttle::async::CoroScope {
    public:
        CompletionScope()
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

        ~CompletionScope() {
            joinAll();
        }

    private:
        shuttle::async::BlockingExecutor m_blockingExecutor;
    };

    shuttle::async::AsyncTask<void> completeTask(
        shuttle::async::CoroScope,
        std::atomic<uint64_t>* completed)
    {
        completed->fetch_add(1, std::memory_order_relaxed);
        co_return;
    }
}

int main() {
    auto executorResult = shuttle::async::ThreadPoolExecutor::create(1, nullptr);
    if (!executorResult) {
        std::cerr << "Failed to create the worker pool\n";
        return 1;
    }

    auto& executor = *executorResult.value();
    CompletionScope scope;
    std::atomic<uint64_t> completed{0};
    std::vector<shuttle::async::AsyncTask<void>> tasks;
    tasks.reserve(TaskCount);

    for (size_t taskIndex = 0; taskIndex < TaskCount; ++taskIndex) {
        auto task = scope.launchAsync(
            completeTask, static_cast<shuttle::async::IExecutor*>(&executor),
            &completed);
        if (!task.has_value()) {
            std::cerr << "Failed to submit task " << taskIndex << '\n';
            return 1;
        }
        tasks.emplace_back(std::move(task));
    }

    const auto deadline = std::chrono::steady_clock::now() + CompletionTimeout;
    while (completed.load(std::memory_order_acquire) != TaskCount
           && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }

    const uint64_t completionCount = completed.load(std::memory_order_acquire);
    if (completionCount != TaskCount) {
        std::cerr << "Timed out waiting for tasks: " << completionCount << " / "
                  << TaskCount << " completed\n" << std::flush;
        std::_Exit(1);
    }

    for (auto& task : tasks) {
        static_cast<void>(task.get(&executor));
    }
    scope.joinAll();

    std::cout << "Thread-pool completion test passed: "
              << completionCount << " tasks completed\n";
    return 0;
}
