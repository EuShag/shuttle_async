//
// Created by Shagu on 12.10.2026.
//
#include <async/coro_scope.hpp>
namespace shuttle::async {
    Status BlockingExecutor::execute(std::coroutine_handle<>) {
        // Жесткий release: публикуем все данные корутины для ждущего потока
        m_ready.store(true, std::memory_order_release);
        m_ready.notify_all();
        return {};
    }

    void BlockingExecutor::waitForSignal() noexcept {
        // Защита от ложных пробуждений (Spurious Wakeups)
        // Если проснулись случайно, а флаг все еще false — уходим спать дальше
        while (!m_ready.load(std::memory_order_acquire)) {
            // Передаем false: засыпаем ТОЛЬКО если m_ready равен false.
            // Если он уже true, wait() мгновенно вернет управление без сна.
            m_ready.wait(false, std::memory_order_acquire);
        }

        // Сбрасываем в false для следующих итераций (например, для joinTask в деструкторе)
        m_ready.store(false, std::memory_order_relaxed);
    }

    IExecutor::ThreadContextGuard BlockingExecutor::initializeThreadContext() noexcept {
        return ThreadContextGuard{{
            .m_threadPinnedExecutor = this,
            .m_dispatcherCoroHandle = std::noop_coroutine()
        }};
    }

    std::coroutine_handle<> JointerTask::Initial::await_suspend(std::coroutine_handle<> coroHandle) const noexcept {
        if (m_scopePtr) {
            // 1. FIRST, register the jointer handle within the scope so that child frames know whom to wake up!
            m_scopePtr->registerJointerTaskHandle(coroHandle, IExecutor::getThreadPinnedExecutor());
            // 2. ONLY AFTER THAT, raise the atomic DEATH_SIGNAL barrier.
            if (auto handle = m_scopePtr->checkCanDestroyed(coroHandle); handle) return handle;
        }
        return IExecutor::getCurrentDispatcherCoroutine();
    }


    CoroScope::CoroScope(
        LaunchOptions options, CoroScope *parentScope,
        IExecutor* mainExecutor, IExecutor* workerExecutor, IAsyncRuntime* asyncRuntime) noexcept:
        m_launch_options(options), m_parentScope(parentScope),
        m_mainExecutor{mainExecutor}, m_threadPoolExecutor{workerExecutor}, m_asyncRuntime{asyncRuntime} {
        if (m_parentScope) {
            m_parentScope->incrementChildCount();
        }
    }

    CoroScope CoroScope::createChild(LaunchOptions options) noexcept {
        return CoroScope{ options, this, m_mainExecutor, m_threadPoolExecutor, m_asyncRuntime };
    }

    CoroScope::~CoroScope() {
        if (m_parentScope) {
            decrementChildCount();
        }
    }

    CoroScope::CoroScope(CoroScope &&other) noexcept:
        m_childCount{ other.m_childCount.exchange(0, std::memory_order_relaxed) },
        m_parentScope{ std::exchange(other.m_parentScope, nullptr) },
        m_jointerTaskHandle{ other.m_jointerTaskHandle.exchange(nullptr, std::memory_order_relaxed) },
        m_mainExecutor{ std::exchange(other.m_mainExecutor, nullptr) },
        m_threadPoolExecutor{ std::exchange(other.m_threadPoolExecutor, nullptr) },
        m_asyncRuntime{ std::exchange(other.m_asyncRuntime, nullptr) } {}

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

    void JoinableScope::joinAll() {
        auto threadContextGuard = m_blockingExecutor.initializeThreadContext();
        registerJointerTaskHandle(std::noop_coroutine(), &m_blockingExecutor);
        if (checkCanDestroyed(std::noop_coroutine()) == nullptr) {
            m_blockingExecutor.waitForSignal();
        }
    }

    JoinableScope::~JoinableScope() {
        joinAll();
    }

    void CoroScope::registerJointerTaskHandle(std::coroutine_handle<> coroHandle, IExecutor* ctx) noexcept {
        m_jointerTaskHandle.store(coroHandle.address(), std::memory_order_relaxed);
        m_threadExecutor = ctx;
    }
}
