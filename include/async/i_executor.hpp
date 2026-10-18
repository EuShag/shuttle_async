//
// Created by Shagu on 12.10.2026.
//

#ifndef SHUTTLEENGINE_IEXECUTER_HPP
#define SHUTTLEENGINE_IEXECUTER_HPP

#include <coroutine>

#include <async/error.hpp>

namespace shuttle::async {

    class IExecutor;

    /**
     * @if RU
     * @brief Не владеющая информация о контексте исполнения текущего потока.
     *
     * Исполнители устанавливают её на worker потоке и восстанавливают прежнее
     * значение при выходе. Указатели и dispatcher handle не продлевают lifetime
     * соответствующих объектов.
     * @endif
     * @if EN
     * @brief Non-owning information about the current thread's execution context.
     *
     * Executors set this on a worker thread and restore the previous value when
     * leaving. Pointers and the dispatcher handle do not extend the lifetime of
     * their corresponding objects.
     * @endif
     */
    struct alignas(64) ThreadContext {
        /// @if RU
        /// Исполнитель пула, общий для нескольких worker-ов.
        /// @endif
        /// @if EN
        /// Pool executor shared by multiple workers.
        /// @endif
        IExecutor* m_sharedExecutor{};
        /// @if RU
        /// Исполнитель, привязанный к текущему потоку.
        /// @endif
        /// @if EN
        /// Executor pinned to the current thread.
        /// @endif
        IExecutor* m_threadPinnedExecutor{};
        /// @if RU
        /// Dispatcher coroutine текущего worker-а; пустой вне диспетчера.
        /// @endif
        /// @if EN
        /// Dispatcher coroutine for the current worker; empty outside a dispatcher.
        /// @endif
        std::coroutine_handle<> m_dispatcherCoroHandle{};
    };

    /**
     * @if RU
     * @brief Интерфейс постановки coroutine handle в очередь исполнения.
     *
     * Реализация определяет поток и момент возобновления. Переданный handle
     * остаётся не владеющим: вызывающая сторона отвечает за lifetime frame;
     * при ошибке постановки корутина не должна считаться переданной исполнителю.
     * @endif
     * @if EN
     * @brief Interface for enqueueing coroutine handles for execution.
     *
     * The implementation determines the thread and time at which resumption
     * occurs. The supplied handle remains non-owning: the caller is responsible
     * for the frame's lifetime. If enqueueing fails, the coroutine must not be
     * considered transferred to the executor.
     * @endif
     */
    class IExecutor {
    public:
        /// @if RU
        /// Виртуальный деструктор интерфейса.
        /// @endif
        /// @if EN
        /// Virtual destructor for the interface.
        /// @endif
        virtual ~IExecutor() = default;
        /**
         * @if RU
         * @brief Ставит корутину на исполнение.
         * @param coro Валидный handle корутины, которой владеет вызывающий код.
         * @return Успех или ошибка, например invalid handle, остановленный
         *         исполнитель или отказ очереди.
         * @note Thread safety: реализация должна принимать одновременные вызовы,
         *       если предназначена для межпоточной отправки.
         * @endif
         * @if EN
         * @brief Schedules a coroutine for execution.
         * @param coro Valid handle to a coroutine owned by the caller.
         * @return Success or an error, such as an invalid handle, a stopped
         *         executor, or a queue rejection.
         * @note Thread safety: implementations intended for cross-thread
         *       submission must support concurrent calls.
         * @endif
         */
        [[nodiscard]] virtual Status execute(std::coroutine_handle<> coro) = 0;

        /**
         * @if RU
         * @brief Возвращает executor, привязанный к текущему потоку, если он установлен.
         * @return Не владеющий указатель либо nullptr вне контекста исполнителя.
         * @endif
         * @if EN
         * @brief Returns the executor pinned to the current thread, if one is set.
         * @return Non-owning pointer, or `nullptr` outside an executor context.
         * @endif
         */
        [[nodiscard]] static IExecutor* getThreadPinnedExecutor() noexcept {
            return currentThreadContext.m_threadPinnedExecutor;
        }

        /**
         * @if RU
         * @brief Возвращает общий executor текущего потока, если он установлен.
         * @return Не владеющий указатель либо nullptr вне контекста исполнителя.
         * @endif
         * @if EN
         * @brief Returns the shared executor for the current thread, if one is set.
         * @return Non-owning pointer, or `nullptr` outside an executor context.
         * @endif
         */
        [[nodiscard]] static IExecutor* getCurrentSharedExecutor() noexcept {
            return currentThreadContext.m_sharedExecutor;
        }

        /**
         * @if RU
         * @brief Возвращает dispatcher coroutine текущего потока.
         * @return Не владеющий handle либо пустой handle вне dispatcher-а.
         * @endif
         * @if EN
         * @brief Returns the dispatcher coroutine for the current thread.
         * @return Non-owning handle, or an empty handle outside a dispatcher.
         * @endif
         */
        [[nodiscard]] static std::coroutine_handle<> getCurrentDispatcherCoroutine() noexcept {
            return currentThreadContext.m_dispatcherCoroHandle;
        }
    protected:
        /**
         * @if RU
         * @brief RAII-область установки TLS-контекста потока.
         *
         * Сохраняет предыдущий контекст и восстанавливает его при разрушении.
         * Объект должен жить строго на потоке, где был создан; копирование запрещено.
         * @endif
         * @if EN
         * @brief RAII scope for setting a thread's TLS context.
         *
         * Saves the previous context and restores it on destruction. The object
         * must remain on the thread where it was created; copying is disabled.
         * @endif
         */
        class [[nodiscard]] ThreadContextGuard {
        public:
            /// @if RU
            /// Устанавливает новый контекст TLS, сохраняя прежний.
            /// @param context Контекст с не владеющими executor/handle указателями.
            /// @endif
            /// @if EN
            /// Sets a new TLS context while saving the previous one.
            /// @param context Context containing non-owning executor/handle pointers.
            /// @endif
            explicit ThreadContextGuard(ThreadContext context) noexcept
                : m_previousContext{currentThreadContext} {
                currentThreadContext = context;
            }
            /// @if RU
            /// Восстанавливает контекст, активный до создания guard.
            /// @endif
            /// @if EN
            /// Restores the context that was active before the guard was created.
            /// @endif
            ~ThreadContextGuard() noexcept {
                currentThreadContext = m_previousContext;
            }
            ThreadContextGuard(const ThreadContextGuard&) = delete;
            ThreadContextGuard& operator=(const ThreadContextGuard&) = delete;

        private:
            /// @if RU
            /// TLS-контекст, активный до создания guard-а.
            /// @endif
            /// @if EN
            /// TLS context that was active before the guard was created.
            /// @endif
            ThreadContext m_previousContext;
        };

    private:
        /// @if RU
        /// Независимый контекст каждого OS-потока; guard восстанавливает прежнее значение.
        /// @endif
        /// @if EN
        /// Per-OS-thread context; the guard restores its previous value.
        /// @endif
        inline static thread_local ThreadContext currentThreadContext = {};
    };

}

#endif //SHUTTLEENGINE_IEXECUTER_HPP
