//
// Created by Shagu on 12.10.2026.
//

#ifndef SHUTTLEENGINE_AWAITERS_HPP
#define SHUTTLEENGINE_AWAITERS_HPP
#include <coroutine>

namespace shuttle::async {

    class CoroScope;

    /**
     * @brief Initial-suspend awaiter for AsyncTask.
     *
     * In Immediate mode, it skips the initial suspension; in other modes, it
     * leaves the task suspended for later execution by an executor. If shared
     * state creation fails, the coroutine body is not executed.
     */
    class InitialAwaiter {
    public:
        /**
         * @param scope Non-owning pointer to the task's scope; it must remain
         *              alive until the coroutine frame completes.
         * @param allocationFailed Indicates that task-state creation failed.
         */
        explicit InitialAwaiter(CoroScope* scope, bool allocationFailed = false) noexcept
            : coroScope{scope}, m_allocationFailed{allocationFailed} {}

        /// @return `true` only if allocation succeeded and the scope selected Immediate mode.
        [[nodiscard]] bool await_ready() const noexcept;

        /// Suspends the coroutine and passes a noop handle to the dispatcher.
        /// @param handle Handle of the task being suspended.
        /// @return Handle for symmetric transfer.
        [[nodiscard]] std::coroutine_handle<> await_suspend(std::coroutine_handle<> handle) const noexcept;

        /// Completes the initial stage without additional work.
        void await_resume() const noexcept {};

    private:
        /// Non-owning scope used to select the initial launch mode.
        CoroScope *coroScope{ nullptr };
        /// Failure to allocate shared state prevents entry into the coroutine body.
        bool m_allocationFailed{ false };
    };

    /**
     * @brief Finalizes AsyncTask and transfers control to the scope or executor.
     *
     * It temporarily retains shared state and publishes completion before the
     * frame is destroyed. State callbacks are passed as type-erased functions so
     * the awaiter does not depend on AsyncTask's template parameter.
     */
    class FinalAwaiter {
    public:
        /// Type-erased function for retaining or releasing state.
        using StateOperation = void(*)(void*) noexcept;
        /// Type-erased completion publisher; `true` indicates a waiter was awakened.
        using StateCompletion = bool(*)(void*) noexcept;

        /**
         * @param scope Non-owning scope of the coroutine frame; `nullptr` is
         *              allowed for a task without a scope.
         */
        FinalAwaiter(
            CoroScope* scope) noexcept
            : m_scope{scope} {}

        /// Always uses await_suspend to perform finalization.
        [[nodiscard]] bool await_ready() const noexcept { return false; }

        /// Publishes completion before frame destruction and performs a symmetric transfer.
        /// @param handle Handle of the coroutine that is completing.
        /// @return Continuation handle or a noop handle.
        [[nodiscard]] std::coroutine_handle<> await_suspend(std::coroutine_handle<> handle) const noexcept;

        /// Completes the final stage without additional work.
        void await_resume() const noexcept {}

    private:
        /// Scope that owns the coroutine; its lifetime covers frame finalization.
        CoroScope* m_scope{ nullptr };
        /// Non-owning state address retained by the retain/release callbacks.
        void* m_state{ nullptr };
        /// Type-erased state-reference management operations.
        StateOperation m_retainState{ nullptr };
        StateOperation m_releaseState{ nullptr };
        /// Type-erased completion publisher, called before frame destruction.
        StateCompletion m_completeState{ nullptr };
    };


}
#endif //SHUTTLEENGINE_AWAITERS_HPP
