#include <async/async_shared_state.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <new>
#include <thread>
#include <utility>

namespace {
    constexpr uint64_t IterationCount = 100'000;

    struct AllocationCounters {
        std::atomic<uint64_t> allocations{0};
        std::atomic<uint64_t> deallocations{0};
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

        [[nodiscard]] T* allocate(size_t count) noexcept {
            if (m_counters) {
                m_counters->allocations.fetch_add(1, std::memory_order_relaxed);
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
        shuttle::async::AsyncFuture<uint64_t> future,
        InlineExecutor& executor,
        std::atomic<uint64_t>& completed,
        std::atomic<bool>& failed,
        uint64_t expected)
    {
        auto awaitable = future.prepareAwaitable(&executor);
        const auto result = co_await std::move(awaitable);
        if (result.isCancelled || result.value != expected) {
            failed.store(true, std::memory_order_relaxed);
        }
        completed.fetch_add(1, std::memory_order_release);
        completed.notify_one();
    }

    uint64_t payloadFor(uint64_t iteration) noexcept {
        uint64_t value = iteration + 0x9e3779b97f4a7c15ULL;
        value ^= value >> 12;
        value ^= value << 25;
        value ^= value >> 27;
        return value * 0x2545f4914f6cdd1dULL;
    }
}

int main() {
    AllocationCounters allocationCounters;
    std::atomic<uint64_t> published{0};
    std::atomic<uint64_t> completed{0};
    std::atomic<uint64_t> produced{0};
    std::atomic<bool> failed{false};
    std::atomic<bool> stopProducer{false};
    shuttle::async::AsyncPromise<uint64_t> pendingPromise;

    std::thread producer([&] {
        for (uint64_t iteration = 0; iteration < IterationCount; ++iteration) {
            const uint64_t publication = iteration + 1;
            auto observed = published.load(std::memory_order_acquire);
            while (observed < publication
                && !stopProducer.load(std::memory_order_acquire)) {
                published.wait(observed, std::memory_order_acquire);
                observed = published.load(std::memory_order_acquire);
            }
            if (stopProducer.load(std::memory_order_acquire)) {
                break;
            }

            auto promise = std::move(pendingPromise);
            promise.setValue(payloadFor(iteration));
            produced.fetch_add(1, std::memory_order_release);
            produced.notify_one();
        }
    });

    InlineExecutor executor;
    for (uint64_t iteration = 0; iteration < IterationCount; ++iteration) {
        auto stateResult = shuttle::async::makeAsyncState<uint64_t>(
            CountingAllocator<std::byte>{&allocationCounters});
        if (!stateResult) {
            failed.store(true, std::memory_order_relaxed);
            stopProducer.store(true, std::memory_order_release);
            published.notify_one();
            break;
        }

        auto consumer = consume(
            std::move(stateResult->future),
            executor,
            completed,
            failed,
            payloadFor(iteration));
        pendingPromise = std::move(stateResult->promise);
        if ((iteration & 1u) == 0) {
            published.store(iteration + 1, std::memory_order_release);
            published.notify_one();
            consumer.handle.resume();
        } else {
            consumer.handle.resume();
            published.store(iteration + 1, std::memory_order_release);
            published.notify_one();
        }

        uint64_t done = completed.load(std::memory_order_acquire);
        while (done < iteration + 1) {
            completed.wait(done, std::memory_order_acquire);
            done = completed.load(std::memory_order_acquire);
        }

        uint64_t producedCount = produced.load(std::memory_order_acquire);
        while (producedCount < iteration + 1) {
            produced.wait(producedCount, std::memory_order_acquire);
            producedCount = produced.load(std::memory_order_acquire);
        }
        consumer.handle.destroy();
    }

    producer.join();

    const uint64_t allocations =
        allocationCounters.allocations.load(std::memory_order_relaxed);
    const uint64_t deallocations =
        allocationCounters.deallocations.load(std::memory_order_relaxed);
    const uint64_t completions = completed.load(std::memory_order_acquire);

    std::cout << "AsyncSharedState stress: " << completions << " / "
              << IterationCount << " races, " << allocations << " allocations, "
              << deallocations << " deallocations\n";

    if (failed.load(std::memory_order_relaxed)
        || completions != IterationCount
        || allocations != IterationCount
        || deallocations != IterationCount) {
        std::cerr << "AsyncSharedState stress test failed\n";
        return 1;
    }

    std::cout << "AsyncSharedState stress test passed\n";
    return 0;
}
