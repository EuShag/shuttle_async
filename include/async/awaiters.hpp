//
// Created by Shagu on 12.10.2026.
//

#ifndef SHUTTLEENGINE_AWAITERS_HPP
#define SHUTTLEENGINE_AWAITERS_HPP
#include <coroutine>

namespace shuttle::async {

    class CoroScope;

    /**
     * @if RU
     * @brief Awaiter начального suspend у AsyncTask.
     *
     * В режиме Immediate пропускает начальную приостановку; в прочих режимах
     * оставляет задачу приостановленной для последующего запуска исполнителем.
     * При неудачном создании shared state корутина не выполняется.
     * @endif
     * @if EN
     * @brief Initial-suspend awaiter for AsyncTask.
     *
     * In Immediate mode, it skips the initial suspension; in other modes, it
     * leaves the task suspended for later execution by an executor. If shared
     * state creation fails, the coroutine body is not executed.
     * @endif
     */
    class InitialAwaiter {
    public:
        /**
         * @if RU
         * @param scope Не владеющий указатель на scope задачи; должен жить до
         *              завершения coroutine frame.
         * @param allocationFailed Указывает, что создание состояния задачи не удалось.
         * @endif
         * @if EN
         * @param scope Non-owning pointer to the task's scope; it must remain
         *              alive until the coroutine frame completes.
         * @param allocationFailed Indicates that task-state creation failed.
         * @endif
         */
        explicit InitialAwaiter(CoroScope* scope, bool allocationFailed = false) noexcept
            : coroScope{scope}, m_allocationFailed{allocationFailed} {}

        /// @if RU
        /// @return `true` только если выделение удалось и scope выбрал Immediate.
        /// @endif
        /// @if EN
        /// @return `true` only if allocation succeeded and the scope selected Immediate mode.
        /// @endif
        [[nodiscard]] bool await_ready() const noexcept;

        /// @if RU
        /// Приостанавливает корутину и передаёт диспетчеру noop handle.
        /// @param handle Coroutine handle приостанавливаемой задачи.
        /// @return Handle для symmetric transfer.
        /// @endif
        /// @if EN
        /// Suspends the coroutine and passes a noop handle to the dispatcher.
        /// @param handle Handle of the task being suspended.
        /// @return Handle for symmetric transfer.
        /// @endif
        [[nodiscard]] std::coroutine_handle<> await_suspend(std::coroutine_handle<> handle) const noexcept;

        /// @if RU
        /// Завершает начальную стадию без дополнительной работы.
        /// @endif
        /// @if EN
        /// Completes the initial stage without additional work.
        /// @endif
        void await_resume() const noexcept {};

    private:
        /// @if RU
        /// Не владеющий scope, используемый для выбора режима начального запуска.
        /// @endif
        /// @if EN
        /// Non-owning scope used to select the initial launch mode.
        /// @endif
        CoroScope *coroScope{ nullptr };
        /// @if RU
        /// Ошибка размещения shared state запрещает вход в coroutine body.
        /// @endif
        /// @if EN
        /// Failure to allocate shared state prevents entry into the coroutine body.
        /// @endif
        bool m_allocationFailed{ false };
    };

    /**
     * @if RU
     * @brief Финализирует AsyncTask и передаёт управление scope/исполнителю.
     *
     * Перед уничтожением frame временно удерживает shared state и публикует
     * завершение. Callback-ы состояния передаются как type-erased функции,
     * чтобы awaiter не зависел от параметра шаблона AsyncTask.
     * @endif
     * @if EN
     * @brief Finalizes AsyncTask and transfers control to the scope or executor.
     *
     * It temporarily retains shared state and publishes completion before the
     * frame is destroyed. State callbacks are passed as type-erased functions so
     * the awaiter does not depend on AsyncTask's template parameter.
     * @endif
     */
    class FinalAwaiter {
    public:
        /// @if RU
        /// Type-erased функция удержания или освобождения состояния.
        /// @endif
        /// @if EN
        /// Type-erased function for retaining or releasing state.
        /// @endif
        using StateOperation = void(*)(void*) noexcept;
        /// @if RU
        /// Type-erased функция публикации завершения; true означает пробуждённого waiter.
        /// @endif
        /// @if EN
        /// Type-erased completion publisher; `true` indicates a waiter was awakened.
        /// @endif
        using StateCompletion = bool(*)(void*) noexcept;

        /**
         * @if RU
         * @param scope Не владеющий scope coroutine frame; nullptr допустим для
         *              задачи без scope.
         * @param state Не владеющий адрес shared state.
         * @param retainState Функция временного удержания state.
         * @param releaseState Функция освобождения временной ссылки.
         * @param completeState Публикует завершение и сообщает, был ли waiter.
         * @endif
         * @if EN
         * @param scope Non-owning scope of the coroutine frame; `nullptr` is
         *              allowed for a task without a scope.
         * @param state Non-owning address of shared state.
         * @param retainState Function that temporarily retains the state.
         * @param releaseState Function that releases the temporary reference.
         * @param completeState Publishes completion and reports whether there was a waiter.
         * @endif
         */
        FinalAwaiter(
            CoroScope* scope,
            void* state,
            StateOperation retainState,
            StateOperation releaseState,
            StateCompletion completeState) noexcept
            : m_scope{scope},
              m_state{state},
              m_retainState{retainState},
              m_releaseState{releaseState},
              m_completeState{completeState} {}

        /// @if RU
        /// Всегда использует await_suspend, чтобы выполнить финализацию.
        /// @endif
        /// @if EN
        /// Always uses await_suspend to perform finalization.
        /// @endif
        [[nodiscard]] bool await_ready() const noexcept { return false; }

        /// @if RU
        /// Публикует завершение до уничтожения frame и выполняет symmetric transfer.
        /// @param handle Завершающийся coroutine handle.
        /// @return Handle продолжения либо noop handle.
        /// @endif
        /// @if EN
        /// Publishes completion before frame destruction and performs a symmetric transfer.
        /// @param handle Handle of the coroutine that is completing.
        /// @return Continuation handle or a noop handle.
        /// @endif
        [[nodiscard]] std::coroutine_handle<> await_suspend(std::coroutine_handle<> handle) const noexcept;

        /// @if RU
        /// Завершает финальную стадию без дополнительной работы.
        /// @endif
        /// @if EN
        /// Completes the final stage without additional work.
        /// @endif
        void await_resume() const noexcept {}

    private:
        /// @if RU
        /// Scope-владелец coroutine; его lifetime охватывает финализацию frame.
        /// @endif
        /// @if EN
        /// Scope that owns the coroutine; its lifetime covers frame finalization.
        /// @endif
        CoroScope* m_scope{ nullptr };
        /// @if RU
        /// Не владеющий адрес состояния, удерживаемый retain/release callbacks.
        /// @endif
        /// @if EN
        /// Non-owning state address retained by the retain/release callbacks.
        /// @endif
        void* m_state{ nullptr };
        /// @if RU
        /// Type-erased операции управления ссылкой state.
        /// @endif
        /// @if EN
        /// Type-erased state-reference management operations.
        /// @endif
        StateOperation m_retainState{ nullptr };
        StateOperation m_releaseState{ nullptr };
        /// @if RU
        /// Type-erased публикация завершения до уничтожения frame.
        /// @endif
        /// @if EN
        /// Type-erased completion publisher, called before frame destruction.
        /// @endif
        StateCompletion m_completeState{ nullptr };
    };


}
#endif //SHUTTLEENGINE_AWAITERS_HPP
