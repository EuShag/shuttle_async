#include <async/coro_scope.hpp>
#include <async/i_executor.hpp>
#include <async/thread_pool_executor/thread_pool_executor.hpp>

#include <cstdint>
#include <iostream>

namespace {
    shuttle::async::AsyncTask<uint64_t> calculate(
        shuttle::async::CoroScope,
        uint64_t input)
    {
        co_return input * 2;
    }
}

int main() {
    using namespace shuttle::async;

    auto executorResult = ThreadPoolExecutor::create(4, nullptr);
    if (!executorResult) {
        std::cerr << "Could not create the thread pool\n";
        return 1;
    }

    JoinableScope scope;
    auto task = scope.launchAsync(
        calculate,
        static_cast<IExecutor*>(executorResult.value().get()),
        uint64_t{21});
    if (!task.has_value()) {
        std::cerr << "Could not launch the task\n";
        return 2;
    }

    const uint64_t result = task.get();
    scope.joinAll();
    std::cout << "Task result: " << result << '\n';
    return result == 42 ? 0 : 3;
}
