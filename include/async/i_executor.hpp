//
// Created by Shagu on 12.10.2026.
//

#ifndef SHUTTLEENGINE_IEXECUTER_HPP
#define SHUTTLEENGINE_IEXECUTER_HPP

#include <coroutine>

#include <async/error.hpp>

#include "async_result.hpp"

namespace shuttle::async {

    class IExecutor;

    enum class NodeState : uint32_t {
        PENDING,   // Нода добавлена в стек, корутина еще выполняет await_suspend
        SUSPEND,   // Корутина официально приостановлена и ждет сигнала
        SIGNALED,  // Поток notify пришел раньше и разрешил не засыпать
        CANCELLED
    };

    struct WaitNode {
        std::coroutine_handle<>     handle{ nullptr };
        IExecutor*                  executor{ nullptr };
        std::atomic<NodeState>      nodeState{ NodeState::PENDING };
        std::atomic_bool           parentCancelledFlag{ false };
    };

    /**
     * @brief Non-owning information about the current thread's execution context.
     *
     * Executors set this on a worker thread and restore the previous value when
     * leaving. Pointers and the dispatcher handle do not extend the lifetime of
     * their corresponding objects.
     */
    struct alignas(64) ThreadContext {
        /// Executor pinned to the current thread.
        IExecutor* m_threadPinnedExecutor{};
        /// Dispatcher coroutine for the current worker; empty outside a dispatcher.
        std::coroutine_handle<> m_dispatcherCoroHandle{};
    };

    /**
     * @brief Interface for enqueueing coroutine handles for execution.
     *
     * The implementation determines the thread and time at which resumption
     * occurs. The supplied handle remains non-owning: the caller is responsible
     * for the frame's lifetime. If enqueueing fails, the coroutine must not be
     * considered transferred to the executor.
     */
    class IExecutor {
    public:
        /// Virtual destructor for the interface.
        virtual ~IExecutor() = default;
        /**
         * @brief Schedules a coroutine for execution.
         * @param coro Valid handle to a coroutine owned by the caller.
         * @return Success or an error, such as an invalid handle, a stopped
         *         executor, or a queue rejection.
         * @note Thread safety: implementations intended for cross-thread
         *       submission must support concurrent calls.
         */
        [[nodiscard]] virtual Status execute(std::coroutine_handle<> coro) = 0;

        /**
         * @brief Returns the executor pinned to the current thread, if one is set.
         * @return Non-owning pointer, or `nullptr` outside an executor context.
         */
        [[nodiscard]] static IExecutor* getThreadPinnedExecutor() noexcept {
            return currentThreadContext.m_threadPinnedExecutor;
        }

        /**
         * @brief Returns the dispatcher coroutine for the current thread.
         * @return Non-owning handle, or an empty handle outside a dispatcher.
         */
        [[nodiscard]] static std::coroutine_handle<> getCurrentDispatcherCoroutine() noexcept {
            return currentThreadContext.m_dispatcherCoroHandle;
        }

        struct SwitchExecutorAwaiter final {
            IExecutor* targetExecutor{ nullptr };

            // 1. Быстрая проверка до приостановки
            bool await_ready(WaitNode* pWaitNode) noexcept {
                return pWaitNode->parentCancelledFlag && pWaitNode->parentCancelledFlag.load(std::memory_order_acquire) && targetExecutor == getThreadPinnedExecutor();
            }

            // 2. Проверка непосредственно в момент планирования в очереди
            bool await_suspend(std::coroutine_handle<> currentCoro, WaitNode* pWaitNode) noexcept {
                // FAST-PATH ЗАЩИТА: Если отмена прилетела прямо во время вызова await_suspend,
                // мы никуда не пушим задачу, а сразу возвращаем текущий хэндл текущему потоку!
                if (pWaitNode->parentCancelledFlag && pWaitNode->parentCancelledFlag.load(std::memory_order_acquire)) [[unlikely]] {
                    return false;
                }

                if (targetExecutor) [[likely]] {
                    // Пушим в lock-free воронку нового экзекьютора с барьером памяти Release
                    if (targetExecutor->execute(currentCoro)) [[likely]] {
                        return true;
                    }
                }
                return false;
            }

            // 3. Финальный замер статуса отмены после пробуждения на новом ядре
            AsyncResult<void> await_resume(WaitNode* pWaitNode) noexcept {
                bool const cancelled = pWaitNode->parentCancelledFlag && pWaitNode->parentCancelledFlag.load(std::memory_order_acquire);
                return AsyncResult<void>{ .isCancelled = cancelled };
            }
        };

    protected:
        /**
         * @brief RAII scope for setting a thread's TLS context.
         *
         * Saves the previous context and restores it on destruction. The object
         * must remain on the thread where it was created; copying is disabled.
         */
        class [[nodiscard]] ThreadContextGuard {
        public:
            /// Sets a new TLS context while saving the previous one.
            /// @param context Context containing non-owning executor/handle pointers.
            explicit ThreadContextGuard(ThreadContext context) noexcept
                : m_previousContext{currentThreadContext} {
                currentThreadContext = context;
            }
            /// Restores the context that was active before the guard was created.
            ~ThreadContextGuard() noexcept {
                currentThreadContext = m_previousContext;
            }
            ThreadContextGuard(const ThreadContextGuard&) = delete;
            ThreadContextGuard& operator=(const ThreadContextGuard&) = delete;

        private:
            /// TLS context that was active before the guard was created.
            ThreadContext m_previousContext;
        };

    private:
        /// Per-OS-thread context; the guard restores its previous value.
        inline static thread_local ThreadContext currentThreadContext = {};
    };

}

#endif //SHUTTLEENGINE_IEXECUTER_HPP
