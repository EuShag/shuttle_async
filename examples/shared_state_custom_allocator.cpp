#include <async/async_shared_state.hpp>

#include <cstddef>
#include <iostream>
#include <new>
#include <utility>

template <typename T>
class ExampleAllocator {
public:
    using value_type = T;

    ExampleAllocator() noexcept = default;

    template <typename U>
    ExampleAllocator(const ExampleAllocator<U>&) noexcept {}

    [[nodiscard]] T* allocate(std::size_t count) {
        return static_cast<T*>(::operator new(
            count * sizeof(T), std::align_val_t{alignof(T)}));
    }

    void deallocate(T* pointer, std::size_t) noexcept {
        ::operator delete(pointer, std::align_val_t{alignof(T)});
    }
};

int main() {
    using namespace shuttle::async;

    auto stateResult = makeAsyncState<int>(ExampleAllocator<std::byte>{});
    if (!stateResult) {
        std::cerr << "Could not allocate shared state\n";
        return 1;
    }

    auto promise = std::move(stateResult->promise);
    auto future = std::move(stateResult->future);
    promise.setValue(42);

    auto awaitable = future.prepareAwaitable(nullptr);
    if (!awaitable.claimAwait()) {
        std::cerr << "Could not claim the future result\n";
        return 2;
    }
    auto awaiter = awaitable.prepareAwaiter(nullptr);
    if (!awaiter.await_ready()) {
        std::cerr << "The already-completed state was not ready\n";
        return 3;
    }

    const auto result = awaiter.await_resume();
    if (result.isCancelled) {
        std::cerr << "Shared state completed as cancelled\n";
        return 4;
    }

    std::cout << "Shared-state result: " << result.value << '\n';
    return result.value == 42 ? 0 : 5;
}
