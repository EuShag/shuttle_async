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
     * @brief Internal coroutine dispatcher type for a worker thread.
     *
     * It owns the handle to its coroutine frame until completion. The dispatcher
     * suspends at yield_value and transfers control to the selected task using a
     * symmetric transfer.
     */
    class DispatcherTask {
    public:
        /// Coroutine promise for creating and scheduling the dispatcher frame.
        struct promise_type {
            /// Creates a wrapper object from the coroutine frame.
            DispatcherTask get_return_object() noexcept;
            /// Returns allocation_failure if frame allocation failed.
            static DispatcherTask get_return_object_on_allocation_failure() noexcept;

            /// Leaves the dispatcher suspended until its worker starts it.
            [[nodiscard]] std::suspend_always initial_suspend() const noexcept { return {}; }

            /// Completes the frame without an additional suspension.
            [[nodiscard]] std::suspend_never final_suspend() const noexcept { return {}; }

            /// Handles a normal co_return.
            void return_void() const noexcept {}

            /// An unhandled exception terminates the process.
            void unhandled_exception() const noexcept { std::terminate(); }

            /**
             * @brief Transfers control to the selected coroutine using symmetric transfer.
             * @param nextHandle Handle of the task for the dispatcher to resume.
             * @return Awaiter that transfers control to this handle.
             */
            [[nodiscard]] auto yield_value(std::coroutine_handle<> nextHandle) const noexcept {
                struct YieldAwaiter {
                    /// Coroutine handle for the next symmetric transfer
                    std::coroutine_handle<> m_handle;

                    /// Always suspends the dispatcher to transfer control.
                    [[nodiscard]] bool await_ready() const noexcept { return false; }
                    /// @return Handle of the task to which control is transferred.
                    [[nodiscard]] std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept {
                        return m_handle;
                    }
                    /// Resumes after the task returns control to the dispatcher.
                    void await_resume() const noexcept {}
                };
                return YieldAwaiter{nextHandle};
            }

            /// Allocates the coroutine frame using MiAllocator; `nullptr` indicates failure.
            void* operator new(std::size_t size) noexcept { return MiAllocator<char>().allocate(size); }
            /// Frees the coroutine frame using MiAllocator.
            void operator delete(void* ptr) noexcept { MiAllocator<char>().deallocate(static_cast<char*>(ptr)); }
        };

        /// Checks whether the wrapper contains a handle rather than an error.
        operator bool() const noexcept { return result.has_value(); }
        /// @return true if the coroutine frame was created successfully.
        [[nodiscard]] bool has_value() const noexcept { return result.has_value(); }
        /// @return Error code; callers must first check has_value().
        [[nodiscard]] Error error() const noexcept { return result.error(); }
        /// Gets the dispatcher frame handle; valid only when has_value() is true.
        operator std::coroutine_handle<>() const noexcept { return result.value(); }

        /// Creates a wrapper that owns the supplied dispatcher handle.
        DispatcherTask(std::coroutine_handle<> handle) noexcept : result(handle) {}
        /// Creates an error wrapper.
        DispatcherTask(Error error) noexcept : result{std::unexpected{error}} {}

    private:
        /// Result containing the dispatcher handle or a frame-creation error.
        Result<std::coroutine_handle<>> result;
    };
}

#endif //SHUTTLEENGINE_THREADPOOLDISPATCHERTASK_HPP
