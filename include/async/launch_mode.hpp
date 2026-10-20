//
// Created by Shagu on 12.10.2026.
//

#ifndef SHUTTLEENGINE_LAUNCHMODE_HPP
#define SHUTTLEENGINE_LAUNCHMODE_HPP

#endif //SHUTTLEENGINE_LAUNCHMODE_HPP

namespace shuttle::async {
    class IExecutor;

    /**
     * @brief Launch mode for a child coroutine created through CoroScope.
     */
    enum class LaunchMode {
        /// Run the coroutine immediately on the current thread.
        Immediate,
        /// Enqueue the coroutine on the specified executor before `launch` returns.
        Async,
        /// Return the created coroutine without starting it; it is started later.
        Lazy
    };

    /**
     * @brief Launch settings for a child task.
     *
     * The executor pointer is non-owning and must remain valid while the task is
     * enqueued and executed. Launch itself does not use it for `Immediate` or
     * `Lazy`; `Async` requires a non-null pointer.
     */
    struct LaunchOptions {
        /// Selected mode; tasks launch asynchronously by default.
        LaunchMode mode{ LaunchMode::Async };
        /// Executor for `Async` mode; `nullptr` results in a launch error.
        IExecutor* explicitExecutor{ nullptr };
    };
} // namespace shuttle::async
