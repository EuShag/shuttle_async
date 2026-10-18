#include <async/async_shared_state.hpp>
#include <async/coro_scope.hpp>
#include <async/thread_pool_executor/thread_pool_executor.hpp>

#include <atomic>
#include <cstddef>
#include <coroutine>
#include <new>
#include <type_traits>

namespace {
    struct AllocationCounters {
        std::atomic<size_t> allocations{0};
        std::atomic<size_t> deallocations{0};
        bool failAllocation{false};
        bool throwAllocation{false};
    };

    template <typename T>
    class CountingAllocator {
    public:
        using value_type = T;

        explicit CountingAllocator(AllocationCounters* counters = nullptr) noexcept
            : m_counters(counters) {}

        template <typename U>
        CountingAllocator(const CountingAllocator<U>& other) noexcept
            : m_counters(other.counters()) {}

        [[nodiscard]] T* allocate(size_t count) {
            if (m_counters) {
                m_counters->allocations.fetch_add(1, std::memory_order_relaxed);
                if (m_counters->throwAllocation) {
                    throw std::bad_alloc{};
                }
                if (m_counters->failAllocation) {
                    return nullptr;
                }
            }
            return static_cast<T*>(::operator new(
                count * sizeof(T), std::align_val_t{alignof(T)}, std::nothrow));
        }

        void deallocate(T* ptr, size_t) noexcept {
            if (!ptr) {
                return;
            }
            if (m_counters) {
                m_counters->deallocations.fetch_add(1, std::memory_order_relaxed);
            }
            ::operator delete(ptr, std::align_val_t{alignof(T)});
        }

        [[nodiscard]] AllocationCounters* counters() const noexcept {
            return m_counters;
        }

    private:
        template <typename> friend class CountingAllocator;
        AllocationCounters* m_counters;
    };

    struct LifetimeCounters {
        size_t live{0};
        size_t destroyed{0};
    };

    struct LifetimeValue {
        LifetimeCounters* counters;
        uint64_t value;

        LifetimeValue(LifetimeCounters& lifetimeCounters, uint64_t storedValue) noexcept
            : counters(&lifetimeCounters), value(storedValue) {
            ++counters->live;
        }

        LifetimeValue(LifetimeValue&& other) noexcept
            : counters(other.counters), value(other.value) {
            ++counters->live;
        }

        LifetimeValue(const LifetimeValue&) = delete;
        LifetimeValue& operator=(const LifetimeValue&) = delete;
        LifetimeValue& operator=(LifetimeValue&&) = delete;

        ~LifetimeValue() {
            --counters->live;
            ++counters->destroyed;
        }
    };

    class InlineExecutor final : public shuttle::async::IExecutor {
    public:
        shuttle::async::Status execute(std::coroutine_handle<> handle) override {
            handle.resume();
            return {};
        }
    };

    struct ConsumerTask {
        struct promise_type {
            ConsumerTask get_return_object() noexcept {
                return ConsumerTask{
                    std::coroutine_handle<promise_type>::from_promise(*this)};
            }

            std::suspend_always initial_suspend() const noexcept { return {}; }
            std::suspend_always final_suspend() const noexcept { return {}; }
            void return_void() const noexcept {}
            void unhandled_exception() const noexcept { std::terminate(); }
        };

        std::coroutine_handle<promise_type> handle;
    };

    ConsumerTask consume(
        shuttle::async::AsyncFuture<uint64_t>::FutureAwaitable awaitable,
        std::atomic<bool>& completed,
        std::atomic<bool>& failed,
        bool expectedCancelled = false)
    {
        const auto result = co_await std::move(awaitable);
        failed.store(
            result.isCancelled != expectedCancelled
                || (!expectedCancelled && result.value != 0xfeedbeefULL),
            std::memory_order_relaxed);
        completed.store(true, std::memory_order_release);
        completed.notify_one();
    }

    shuttle::async::AsyncTask<uint64_t> returnValueTask(
        shuttle::async::CoroScope,
        uint64_t value)
    {
        co_return value;
    }

    static_assert(!std::is_default_constructible_v<shuttle::async::AsyncSharedState<int>>);
    static_assert(!std::is_copy_constructible_v<shuttle::async::AsyncPromise<int>>);
    static_assert(std::is_nothrow_move_constructible_v<shuttle::async::AsyncPromise<int>>);
    static_assert(!std::is_copy_constructible_v<shuttle::async::AsyncFuture<int>>);
    static_assert(std::is_nothrow_move_constructible_v<shuttle::async::AsyncFuture<int>>);
}

int main() {
    AllocationCounters counters;
    {
        auto stateResult = shuttle::async::makeAsyncState<uint64_t>(
            CountingAllocator<std::byte>{&counters});
        if (!stateResult) {
            return 1;
        }

        auto promise = std::move(stateResult->promise);
        auto future = std::move(stateResult->future);
        promise.setValue(uint64_t{0x123456789abcdef0});

        auto awaitable = future.prepareAwaitable(nullptr);
        if (!awaitable.claimAwait()) {
            return 2;
        }
        auto awaiter = awaitable.prepareAwaiter(nullptr);
        if (!awaiter.await_ready()) {
            return 3;
        }
        const auto result = awaiter.await_resume();
        if (result.isCancelled || result.value != uint64_t{0x123456789abcdef0}) {
            return 4;
        }
    }

    if (counters.allocations.load(std::memory_order_relaxed) != 1
        || counters.deallocations.load(std::memory_order_relaxed) != 1) {
        return 5;
    }

    AllocationCounters failedAllocationCounters;
    failedAllocationCounters.failAllocation = true;
    auto failedState = shuttle::async::makeAsyncState<uint64_t>(
        CountingAllocator<std::byte>{&failedAllocationCounters});
    if (failedState || failedState.error() != shuttle::async::Error::allocation_failure
        || failedAllocationCounters.allocations.load(std::memory_order_relaxed) != 1
        || failedAllocationCounters.deallocations.load(std::memory_order_relaxed) != 0) {
        return 6;
    }

    AllocationCounters throwingAllocationCounters;
    throwingAllocationCounters.throwAllocation = true;
    auto throwingState = shuttle::async::makeAsyncState<uint64_t>(
        CountingAllocator<std::byte>{&throwingAllocationCounters});
    if (throwingState
        || throwingState.error() != shuttle::async::Error::allocation_failure
        || throwingAllocationCounters.allocations.load(std::memory_order_relaxed) != 1
        || throwingAllocationCounters.deallocations.load(std::memory_order_relaxed) != 0) {
        return 20;
    }

    AllocationCounters retainedStateCounters;
    std::atomic<bool> consumerCompleted{false};
    std::atomic<bool> consumerFailed{false};
    InlineExecutor executor;
    ConsumerTask consumer;
    shuttle::async::AsyncPromise<uint64_t> producer;
    {
        auto stateResult = shuttle::async::makeAsyncState<uint64_t>(
            CountingAllocator<std::byte>{&retainedStateCounters});
        if (!stateResult) {
            return 7;
        }
        producer = std::move(stateResult->promise);
        auto future = std::move(stateResult->future);
        auto awaitable = future.prepareAwaitable(&executor);
        consumer = consume(std::move(awaitable), consumerCompleted, consumerFailed);
    }
    consumer.handle.resume();
    if (consumer.handle.done()
        || retainedStateCounters.deallocations.load(std::memory_order_relaxed) != 0) {
        return 8;
    }
    producer.setValue(uint64_t{0xfeedbeefULL});
    producer = {};
    if (!consumerCompleted.load(std::memory_order_acquire)
        || consumerFailed.load(std::memory_order_relaxed)
        || retainedStateCounters.deallocations.load(std::memory_order_relaxed) != 0) {
        return 9;
    }
    consumer.handle.destroy();
    if (retainedStateCounters.allocations.load(std::memory_order_relaxed) != 1
        || retainedStateCounters.deallocations.load(std::memory_order_relaxed) != 1) {
        return 10;
    }

    AllocationCounters abandonedStateCounters;
    consumerCompleted.store(false, std::memory_order_relaxed);
    consumerFailed.store(false, std::memory_order_relaxed);
    {
        auto stateResult = shuttle::async::makeAsyncState<uint64_t>(
            CountingAllocator<std::byte>{&abandonedStateCounters});
        if (!stateResult) {
            return 11;
        }
        auto future = std::move(stateResult->future);
        auto awaitable = future.prepareAwaitable(&executor);
        consumer = consume(
            std::move(awaitable), consumerCompleted, consumerFailed, true);
        consumer.handle.resume();
        stateResult->promise = {};
    }
    if (!consumerCompleted.load(std::memory_order_acquire)
        || consumerFailed.load(std::memory_order_relaxed)
        || abandonedStateCounters.deallocations.load(std::memory_order_relaxed) != 0) {
        return 12;
    }
    consumer.handle.destroy();
    if (abandonedStateCounters.allocations.load(std::memory_order_relaxed) != 1
        || abandonedStateCounters.deallocations.load(std::memory_order_relaxed) != 1) {
        return 13;
    }

    AllocationCounters emptyAbandonedStateCounters;
    LifetimeCounters abandonedLifetime;
    {
        auto stateResult = shuttle::async::makeAsyncState<LifetimeValue>(
            CountingAllocator<std::byte>{&emptyAbandonedStateCounters});
        if (!stateResult) {
            return 14;
        }
        stateResult->promise = {};
        stateResult->future = {};
    }
    if (abandonedLifetime.live != 0 || abandonedLifetime.destroyed != 0
        || emptyAbandonedStateCounters.allocations.load(std::memory_order_relaxed) != 1
        || emptyAbandonedStateCounters.deallocations.load(std::memory_order_relaxed) != 1) {
        return 15;
    }

    AllocationCounters valueLifetimeCounters;
    LifetimeCounters lifetime;
    {
        auto stateResult = shuttle::async::makeAsyncState<LifetimeValue>(
            CountingAllocator<std::byte>{&valueLifetimeCounters});
        if (!stateResult) {
            return 16;
        }
        auto promise = std::move(stateResult->promise);
        auto future = std::move(stateResult->future);
        promise.setValue(LifetimeValue{lifetime, 73});
        promise = {};
        future = {};
    }
    if (lifetime.live != 0 || lifetime.destroyed != 2
        || valueLifetimeCounters.allocations.load(std::memory_order_relaxed) != 1
        || valueLifetimeCounters.deallocations.load(std::memory_order_relaxed) != 1) {
        return 17;
    }

    auto executorResult = shuttle::async::ThreadPoolExecutor::create(2, nullptr);
    if (!executorResult) {
        return 18;
    }
    {
        shuttle::async::JoinableScope scope;
        auto task = scope.launchAsync(
            returnValueTask,
            static_cast<shuttle::async::IExecutor*>(executorResult.value().get()),
            uint64_t{0xabad1deaULL});
        if (!task.has_value() || task.get() != uint64_t{0xabad1deaULL}) {
            return 19;
        }
        scope.joinAll();
    }

    return 0;
}
