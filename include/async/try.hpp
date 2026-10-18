#pragma once

#include <async/error.hpp>

#include <utility>

/**
 * @if RU
 * @brief Возвращает ошибку выражения из текущей функции Status-returning.
 *
 * Оценивает expression ровно один раз; при неуспехе возвращает
 * std::unexpected(error()), иначе продолжает выполнение.
 * @endif
 * @if EN
 * @brief Propagates an expression's error from the current Status-returning function.
 *
 * Evaluates expression exactly once; on failure, returns
 * std::unexpected(error()), otherwise execution continues.
 * @endif
 */
#define ASYNC_TRY_VOID(expression)                                           \
    do {                                                                     \
        auto _async_result = (expression);                                   \
        if (!_async_result) [[unlikely]] {                                   \
            return ::std::unexpected(_async_result.error());                \
        }                                                                    \
    } while (false)

#define ASYNC_DETAIL_CONCAT_INNER(lhs, rhs) lhs##rhs
#define ASYNC_DETAIL_CONCAT(lhs, rhs) ASYNC_DETAIL_CONCAT_INNER(lhs, rhs)
/**
 * @if RU
 * @brief Реализация ASYNC_TRY с уникальным именем временного результата.
 * @internal
 * @endif
 * @if EN
 * @brief Implementation of ASYNC_TRY using a unique temporary result name.
 * @internal
 * @endif
 */
#define ASYNC_DETAIL_TRY(variable, expression, result_name)                  \
    auto result_name = (expression);                                         \
    if (!result_name) [[unlikely]] {                                         \
        return ::std::unexpected(result_name.error());                      \
    }                                                                        \
    auto variable = std::move(result_name).value()

/**
 * @if RU
 * @brief Проверяет Result и извлекает его значение в новую переменную.
 *
 * При ошибке немедленно возвращает std::unexpected(error()) из текущей функции.
 * @param variable Имя локальной переменной для значения.
 * @param expression Выражение, возвращающее Result-подобный объект.
 * @endif
 * @if EN
 * @brief Checks a Result and extracts its value into a new variable.
 *
 * On error, immediately returns std::unexpected(error()) from the current function.
 * @param variable Name of the local variable that receives the value.
 * @param expression Expression that returns a Result-like object.
 * @endif
 */
#define ASYNC_TRY(variable, expression)                                       \
    ASYNC_DETAIL_TRY(variable, expression, ASYNC_DETAIL_CONCAT(_async_result_, variable))
