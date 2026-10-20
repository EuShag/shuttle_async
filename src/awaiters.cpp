//
// Created by Shagu on 12.10.2026.
//

#include <async/awaiters.hpp>
#include <async/coro_scope.hpp>

namespace shuttle::async {
    bool InitialAwaiter::await_ready() const noexcept {
        return !m_allocationFailed && coroScope->canImmediateLaunched();
    }

    // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
    std::coroutine_handle<> InitialAwaiter::await_suspend(std::coroutine_handle<>) const noexcept {
        return std::noop_coroutine();
    }

    std::coroutine_handle<> FinalAwaiter::await_suspend(std::coroutine_handle<> handle) const noexcept {
        CoroScope* const scope = m_scope;
        void* const state = m_state;
        StateOperation const retainState = m_retainState;
        StateOperation const releaseState = m_releaseState;
        StateCompletion const completeState = m_completeState;

        if (state != nullptr) {
            retainState(state);
        }

        const bool resumedWaiter =
            state != nullptr && completeState(state);

        std::coroutine_handle<> transferHandle;
        if (scope) [[likely]] {
            transferHandle = scope->getTransferCoro(handle);
        } else {
            handle.destroy();
            transferHandle = IExecutor::getCurrentDispatcherCoroutine();
        }

        if (state != nullptr) {
            releaseState(state);
        }

        if (resumedWaiter) {
            auto const dispatcher = IExecutor::getCurrentDispatcherCoroutine();
            return dispatcher ? dispatcher : std::noop_coroutine();
        }
        return transferHandle ? transferHandle : std::noop_coroutine();
    }
}