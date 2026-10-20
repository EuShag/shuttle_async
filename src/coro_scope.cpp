//
// Created by Shagu on 12.10.2026.
//
#include <async/coro_scope.hpp>
namespace shuttle::async {
    std::coroutine_handle<> JointerTask::Initial::await_suspend(std::coroutine_handle<> coroHandle) const noexcept {
        if (m_scopePtr) {
            // 1. FIRST, register the jointer handle within the scope so that child frames know whom to wake up!
            m_scopePtr->registerJointerTaskHandle(coroHandle, IExecutor::getThreadPinnedExecutor());
            // 2. ONLY AFTER THAT, raise the atomic DEATH_SIGNAL barrier.
            if (auto handle = m_scopePtr->checkCanDestroyed(coroHandle); handle) return handle;
        }
        return IExecutor::getCurrentDispatcherCoroutine();
    }


    CoroScope CoroScope::createChild(LaunchOptions options) noexcept {
        return CoroScope{ options, this };
    }

    bool CoroScope::canImmediateLaunched() const noexcept { return m_launch_options.mode == LaunchMode::Immediate; }

    std::coroutine_handle<> CoroScope::getTransferCoro(std::coroutine_handle<> coroutineHandle) const noexcept {
        // 1. Extract the launch mode IN ADVANCE, while the coroutine frame is guaranteed to be intact.
        auto launchMode = m_launch_options.mode;
        coroutineHandle.destroy();
        // 3. Return the parent execution context (or pass control to the dispatcher if this is the root).
        if (launchMode == LaunchMode::Immediate) return std::noop_coroutine();
        return IExecutor::getCurrentDispatcherCoroutine();
    }

    std::coroutine_handle<> CoroScope::checkCanDestroyed(std::coroutine_handle<> coroHandle) noexcept {
        if (uint64_t const prevChildren = m_childCount.fetch_or(DEATH_SIGNAL, std::memory_order_acq_rel);
            (prevChildren & ~DEATH_SIGNAL) == 0) {
            // 2. We are the last ones; the atomic barrier has been successfully passed. Destroy the heap frame.
            return coroHandle;
            }
        return nullptr;
    }

    void CoroScope::incrementChildCount() noexcept {
        m_childCount.fetch_add(1, std::memory_order_relaxed);
    }

    void CoroScope::decrementChildCount() noexcept {
        CoroScope* const parent = m_parentScope;
        if (uint64_t const prev = parent->m_childCount.fetch_sub(1, std::memory_order_acq_rel);
            prev == DEATH_SIGNAL + 1) {
            auto const resumeResult = parent->m_threadExecutor->execute(
                std::coroutine_handle<>::from_address(
                    parent->m_jointerTaskHandle.load(std::memory_order_acquire)));
            if (!resumeResult) [[unlikely]] {
                std::terminate();
            }
        }
    }

    void CoroScope::registerCoroHandle(std::coroutine_handle<> coroHandle) noexcept {
        m_ownerHandle.store(coroHandle.address(), std::memory_order_release);
    }

    std::coroutine_handle<> CoroScope::getCoroHandle() const noexcept {
        void* const addr = m_ownerHandle.load(std::memory_order_acquire);
        return std::coroutine_handle<>::from_address(addr);
    }

    void CoroScope::registerJointerTaskHandle(std::coroutine_handle<> coroHandle, IExecutor* ctx) noexcept {
        m_jointerTaskHandle.store(coroHandle.address(), std::memory_order_relaxed);
        m_threadExecutor = ctx;
    }
}
