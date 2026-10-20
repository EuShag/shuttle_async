#pragma once
#include <concepts>
#include <utility>

namespace shuttle::async {

    /**
     * @brief Result of awaiting an asynchronous operation.
     *
     * The value is returned by move, and cancellation is represented by a
     * separate flag; this type does not encode launch or executor errors. Do not
     * use the value after cancellation. This type does not synchronize access
     * and does not provide thread safety by itself.
     *
     *
     * @tparam T Type of the result value.
     */
    template <typename T>
    struct AsyncResult final {
        /// Operation value; after cancellation, its contents are not the operation's result.
        T value;
        /// `true` if the operation or the coroutine awaiting it was cancelled.
        bool isCancelled{ false };

        /**
         * @brief Checks whether the result is marked as cancelled.
         * @return `true` on normal completion; otherwise, `false`.
         */
        constexpr explicit operator bool() const noexcept { return !isCancelled; }
    };

    /**
     * @brief Result of an operation with no value.
     *
     * @tparam void Indicates that there is no useful value; only the cancellation flag is available.
     */
    template <>
    struct AsyncResult<void> final {
        /// `true` if the operation or the coroutine awaiting it was cancelled.
        bool isCancelled{ false };

        /**
         * @brief Checks whether the result is marked as cancelled.
         * @return `true` on normal completion; otherwise, `false`.
         */
        constexpr explicit operator bool() const noexcept { return !isCancelled; }
    };

} // namespace shuttle::async
