#pragma once
#include <concepts>
#include <utility>

namespace shuttle::async {

    /**
     * @if RU
     * @brief Результат ожидания асинхронной операции.
     *
     * Значение возвращается перемещением, а отмена представлена отдельным флагом;
     * ошибки запуска/исполнителя этим типом не кодируются. При отмене значение
     * не следует использовать. Тип не синхронизирует доступ и сам по себе не
     * обеспечивает потокобезопасность.
     *
     * @tparam T Тип значения результата.
     * @endif
     * @if EN
     * @brief Result of awaiting an asynchronous operation.
     *
     * The value is returned by move, and cancellation is represented by a
     * separate flag; this type does not encode launch or executor errors. Do not
     * use the value after cancellation. This type does not synchronize access
     * and does not provide thread safety by itself.
     *
     * @tparam T Type of the result value.
     * @endif
     */
    template <typename T>
    struct AsyncResult final {
        /// @if RU
        /// Значение операции; при отмене содержимое не является результатом операции.
        /// @endif
        /// @if EN
        /// Operation value; after cancellation, its contents are not the operation's result.
        /// @endif
        T value;
        /// @if RU
        /// `true`, если операция или ожидающая её корутина была отменена.
        /// @endif
        /// @if EN
        /// `true` if the operation or the coroutine awaiting it was cancelled.
        /// @endif
        bool isCancelled{ false };

        /**
         * @if RU
         * @brief Проверяет, не отмечен ли результат как отменённый.
         * @return `true` при обычном завершении, иначе `false`.
         * @endif
         * @if EN
         * @brief Checks whether the result is marked as cancelled.
         * @return `true` on normal completion; otherwise, `false`.
         * @endif
         */
        constexpr explicit operator bool() const noexcept { return !isCancelled; }
    };

    /**
     * @if RU
     * @brief Результат операции без значения.
     *
     * @tparam void Отсутствие полезного значения; доступен только флаг отмены.
     * @endif
     * @if EN
     * @brief Result of an operation with no value.
     *
     * @tparam void Indicates that there is no useful value; only the cancellation flag is available.
     * @endif
     */
    template <>
    struct AsyncResult<void> final {
        /// @if RU
        /// `true`, если операция или ожидающая её корутина была отменена.
        /// @endif
        /// @if EN
        /// `true` if the operation or the coroutine awaiting it was cancelled.
        /// @endif
        bool isCancelled{ false };

        /**
         * @if RU
         * @brief Проверяет, не отмечен ли результат как отменённый.
         * @return `true` при обычном завершении, иначе `false`.
         * @endif
         * @if EN
         * @brief Checks whether the result is marked as cancelled.
         * @return `true` on normal completion; otherwise, `false`.
         * @endif
         */
        constexpr explicit operator bool() const noexcept { return !isCancelled; }
    };

} // namespace shuttle::async
