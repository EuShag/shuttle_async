#include <async/one_thread_executor.hpp>
#include <iostream>
#include <ostream>
#include <thread>

#include "async/coro_scope.hpp"
#include "async/thread_pool_executor/thread_pool_executor.hpp"

shuttle::async::AsyncTask<int> test(shuttle::async::CoroScope, int& i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    std::cout << "I run!: " << i << std::endl;
    co_return 1234;
}

shuttle::async::AsyncTask<int> test1(shuttle::async::CoroScope scope) {

    int i = 3;
    auto coro = scope.launchAsync(test, scope.getThreadPoolExecutor(), i);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    std::cout << "I run1" << std::endl;
    auto valueRes = co_await coro.get();
    auto value = valueRes.value;
    std::cout << value << std::endl;
    co_return 1;
}

shuttle::async::AsyncTask<int> test2(shuttle::async::CoroScope) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    std::cout << "I run2" << std::endl;
    co_return 2;
}

int main() {

    auto executorResult = shuttle::async::ThreadPoolExecutor::create(5, nullptr);
    auto& executor = *executorResult.value();

    shuttle::async::JoinableScope scope{nullptr, &executor, nullptr};

    auto task1 = scope.launchAsync(test1, &executor);
    auto task2 = scope.launchAsync(test2, &executor);

    std::cout << task1.get() << std::endl;
    std::cout << task2.get() << std::endl;

    return 0;
}
