#pragma once

#include <expected>
#include <string_view>

namespace shuttle::async {

    /**
     * @if RU
     * @brief Ошибки создания, постановки и исполнения асинхронных задач.
     *
     * Ошибки передаются как `std::unexpected` внутри `Result`/`Status`;
     * они отличаются от штатной отмены, которая представлена AsyncResult.
     * @endif
     * @if EN
     * @brief Errors that occur while creating, enqueuing, and executing asynchronous tasks.
     *
     * Errors are carried as `std::unexpected` in `Result`/`Status`; they are
     * distinct from normal cancellation, which is represented by AsyncResult.
     * @endif
     */
    enum class Error {
        /// @if RU
        /// Не удалось выделить память для объекта или очереди.
        /// @endif
        /// @if EN
        /// Failed to allocate memory for an object or queue.
        /// @endif
        allocation_failure,
        /// @if RU
        /// Передано нулевое или иное недопустимое число worker-потоков.
        /// @endif
        /// @if EN
        /// A zero or otherwise invalid number of worker threads was provided.
        /// @endif
        invalid_worker_count,
        /// @if RU
        /// Не удалось создать поток исполнителя.
        /// @endif
        /// @if EN
        /// Failed to create an executor thread.
        /// @endif
        worker_start_failed,
        /// @if RU
        /// Передан пустой или недействительный coroutine handle.
        /// @endif
        /// @if EN
        /// An empty or invalid coroutine handle was provided.
        /// @endif
        invalid_coroutine_handle,
        /// @if RU
        /// Исполнитель уже остановлен и не принимает работу.
        /// @endif
        /// @if EN
        /// The executor has stopped and no longer accepts work.
        /// @endif
        executor_stopped,
        /// @if RU
        /// Очередь исполнителя отказала при постановке задачи.
        /// @endif
        /// @if EN
        /// The executor queue rejected a task during enqueue.
        /// @endif
        queue_enqueue_failed,
        /// @if RU
        /// Для асинхронного запуска или ожидания не найден исполнитель.
        /// @endif
        /// @if EN
        /// No executor was found for asynchronous launch or awaiting.
        /// @endif
        executor_unavailable
    };

    /**
     * @if RU
     * @brief Возвращает краткое диагностическое имя ошибки.
     * @param error Код ошибки.
     * @return Статическая строка на английском языке; для неизвестного значения —
     *         `"unknown async error"`.
     * @endif
     * @if EN
     * @brief Returns a short diagnostic name for an error.
     * @param error Error code.
     * @return A static string in English; `"unknown async error"` for an unknown value.
     * @endif
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
     * @if RU
     * @brief Ожидаемый результат, содержащий значение `T` либо `Error`.
     * @tparam T Тип значения при успехе.
     * @endif
     * @if EN
     * @brief Expected result containing either a value of type `T` or an `Error`.
     * @tparam T Type of the value on success.
     * @endif
     */
    template <typename T>
    using Result = std::expected<T, Error>;

    /// @if RU
    /// Результат операции без возвращаемого значения либо ошибка `Error`.
    /// @endif
    /// @if EN
    /// Result of an operation with no return value, or an `Error`.
    /// @endif
    using Status = Result<void>;

}
