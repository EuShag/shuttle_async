#pragma once

#include <expected>
#include <string_view>

namespace shuttle::async {

    /**
     * @brief Errors that occur while creating, enqueuing, and executing asynchronous tasks.
     *
     * Errors are carried as `std::unexpected` in `Result`/`Status`; they are
     * distinct from normal cancellation, which is represented by AsyncResult.
     */
    enum class Error {
        /// Failed to allocate memory for an object or queue.
        allocation_failure,
        /// A zero or otherwise invalid number of worker threads was provided.
        invalid_worker_count,
        /// Failed to create an executor thread.
        worker_start_failed,
        /// An empty or invalid coroutine handle was provided.
        invalid_coroutine_handle,
        /// The executor has stopped and no longer accepts work.
        executor_stopped,
        /// The executor queue rejected a task during enqueue.
        queue_enqueue_failed,
        /// No executor was found for asynchronous launch or awaiting.
        executor_unavailable
    };

    /**
     * @brief Returns a short diagnostic name for an error.
     * @param error Error code.
     * @return A static string in English; `"unknown async error"` for an unknown value.
     */
    [[nodiscard]] constexpr std::string_view to_string(Error error) noexcept {
        switch (error) {
        case Error::allocation_failure: return "allocation failure";
        case Error::invalid_worker_count: return "invalid worker count";
        case Error::worker_start_failed: return "worker start failed";
        case Error::invalid_coroutine_handle: return "invalid coroutine handle";
        case Error::executor_stopped: return "executor stopped";
        case Error::queue_enqueue_failed: return "queue enqueue failed";
        case Error::executor_unavailable: return "executor unavailable";
        }
        return "unknown async error";
    }

    /**
     * @brief Expected result containing either a value of type `T` or an `Error`.
     * @tparam T Type of the value on success.
     */
    template <typename T>
    using Result = std::expected<T, Error>;

    /// Result of an operation with no return value, or an `Error`.
    using Status = Result<void>;

}
