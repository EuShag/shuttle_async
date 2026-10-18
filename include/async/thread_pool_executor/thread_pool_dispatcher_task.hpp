//
// Created by Shagu on 16.10.2026.
//

#ifndef SHUTTLEENGINE_THREADPOOLDISPATCHERTASK_HPP
#define SHUTTLEENGINE_THREADPOOLDISPATCHERTASK_HPP
#include <coroutine>
#include <utility>

#include <async/error.hpp>
#include <async/mi_allocator.hpp>

namespace shuttle::async {
    /**
     * @if RU
     * @brief Внутренний coroutine тип-диспетчер для worker-потока.
     *
     * Владеет handle своей coroutine frame до завершения; диспетчер приостанавливается
     * на yield_value, передавая управление выбранной задаче через symmetric transfer.
     * @endif
     * @if EN
     * @brief Internal coroutine dispatcher type for a worker thread.
     *
     * It owns the handle to its coroutine frame until completion. The dispatcher
     * suspends at yield_value and transfers control to the selected task using a
     * symmetric transfer.
     * @endif
     */
    class DispatcherTask {
    public:
        /// @if RU
        /// Coroutine promise для создания и планирования frame диспетчера.
        /// @endif
        /// @if EN
        /// Coroutine promise for creating and scheduling the dispatcher frame.
        /// @endif
        struct promise_type {
            /// @if RU
            /// Создаёт объект-обёртку из coroutine frame.
            /// @endif
            /// @if EN
            /// Creates a wrapper object from the coroutine frame.
            /// @endif
            DispatcherTask get_return_object() noexcept;
            /// @if RU
            /// Возвращает ошибку allocation_failure, если не удалась аллокация frame.
            /// @endif
            /// @if EN
            /// Returns allocation_failure if frame allocation failed.
            /// @endif
            static DispatcherTask get_return_object_on_allocation_failure() noexcept;

            /// @if RU
            /// Оставляет dispatcher suspended до запуска его worker-ом.
            /// @endif
            /// @if EN
            /// Leaves the dispatcher suspended until its worker starts it.
            /// @endif
            [[nodiscard]] std::suspend_always initial_suspend() const noexcept { return {}; }

            /// @if RU
            /// Завершает frame без дополнительной приостановки.
            /// @endif
            /// @if EN
            /// Completes the frame without an additional suspension.
            /// @endif
            [[nodiscard]] std::suspend_never final_suspend() const noexcept { return {}; }

            /// @if RU
            /// Обрабатывает штатный co_return.
            /// @endif
            /// @if EN
            /// Handles a normal co_return.
            /// @endif
            void return_void() const noexcept {}

            /// @if RU
            /// Необработанное исключение фатально завершает процесс.
            /// @endif
            /// @if EN
            /// An unhandled exception terminates the process.
            /// @endif
            void unhandled_exception() const noexcept { std::terminate(); }

            /**
             * @if RU
             * @brief Передаёт управление выбранной coroutine через symmetric transfer.
             * @param nextHandle Handle задачи, которую должен возобновить dispatcher.
             * @return Awaiter, передающий управление этому handle.
             * @endif
             * @if EN
             * @brief Transfers control to the selected coroutine using symmetric transfer.
             * @param nextHandle Handle of the task for the dispatcher to resume.
             * @return Awaiter that transfers control to this handle.
             * @endif
             */
            [[nodiscard]] auto yield_value(std::coroutine_handle<> nextHandle) const noexcept {
                struct YieldAwaiter {
                    /// @if RU
                    /// Handle корутины для следующего symmetric transfer.
                    /// @endif
                    /// @if EN
                    /// Coroutine handle for the next symmetric transfer.
                    /// @endif
                    std::coroutine_handle<> m_handle;

                    /// @if RU
                    /// Всегда приостанавливает dispatcher для передачи управления.
                    /// @endif
                    /// @if EN
                    /// Always suspends the dispatcher to transfer control.
                    /// @endif
                    [[nodiscard]] bool await_ready() const noexcept { return false; }
                    /// @if RU
                    /// @return Handle задачи, которой передаётся управление.
                    /// @endif
                    /// @if EN
                    /// @return Handle of the task to which control is transferred.
                    /// @endif
                    [[nodiscard]] std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept {
                        return m_handle;
                    }
                    /// @if RU
                    /// Возобновляется после возврата задачи к dispatcher-у.
                    /// @endif
                    /// @if EN
                    /// Resumes after the task returns control to the dispatcher.
                    /// @endif
                    void await_resume() const noexcept {}
                };
                return YieldAwaiter{nextHandle};
            }

            /// @if RU
            /// Выделяет coroutine frame через MiAllocator; nullptr означает отказ.
            /// @endif
            /// @if EN
            /// Allocates the coroutine frame using MiAllocator; `nullptr` indicates failure.
            /// @endif
            void* operator new(std::size_t size) noexcept { return MiAllocator<char>().allocate(size); }
            /// @if RU
            /// Освобождает coroutine frame через MiAllocator.
            /// @endif
            /// @if EN
            /// Frees the coroutine frame using MiAllocator.
            /// @endif
            void operator delete(void* ptr) noexcept { MiAllocator<char>().deallocate(static_cast<char*>(ptr)); }
        };

        /// @if RU
        /// Проверяет, содержит ли обёртка handle, а не ошибку.
        /// @endif
        /// @if EN
        /// Checks whether the wrapper contains a handle rather than an error.
        /// @endif
        operator bool() const noexcept { return result.has_value(); }
        /// @if RU
        /// @return true, если coroutine frame успешно создан.
        /// @endif
        /// @if EN
        /// @return true if the coroutine frame was created successfully.
        /// @endif
        [[nodiscard]] bool has_value() const noexcept { return result.has_value(); }
        /// @if RU
        /// @return Код ошибки; вызывающий должен предварительно проверить has_value().
        /// @endif
        /// @if EN
        /// @return Error code; callers must first check has_value().
        /// @endif
        [[nodiscard]] Error error() const noexcept { return result.error(); }
        /// @if RU
        /// Получает handle dispatcher frame; допустимо только при has_value().
        /// @endif
        /// @if EN
        /// Gets the dispatcher frame handle; valid only when has_value() is true.
        /// @endif
        operator std::coroutine_handle<>() const noexcept { return result.value(); }

        /// @if RU
        /// Создаёт обёртку, владеющую переданным dispatcher handle.
        /// @endif
        /// @if EN
        /// Creates a wrapper that owns the supplied dispatcher handle.
        /// @endif
        DispatcherTask(std::coroutine_handle<> handle) noexcept : result(handle) {}
        /// @if RU
        /// Создаёт ошибочную обёртку.
        /// @endif
        /// @if EN
        /// Creates an error wrapper.
        /// @endif
        DispatcherTask(Error error) noexcept : result{std::unexpected{error}} {}

    private:
        /// @if RU
        /// Результат содержит dispatcher handle либо ошибку создания frame.
        /// @endif
        /// @if EN
        /// Result containing the dispatcher handle or a frame-creation error.
        /// @endif
        Result<std::coroutine_handle<>> result;
    };
}

#endif //SHUTTLEENGINE_THREADPOOLDISPATCHERTASK_HPP
