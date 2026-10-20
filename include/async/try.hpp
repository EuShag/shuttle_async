#pragma once

#include <async/error.hpp>

#include <utility>

/**
 * @brief Propagates an expression's error from the current Status-returning function.
 *
 * Evaluates expression exactly once; on failure, returns
 * std::unexpected(error()), otherwise execution continues.
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
 * @brief Implementation of ASYNC_TRY using a unique temporary result name.
 * @internal
 */
#define ASYNC_DETAIL_TRY(variable, expression, result_name)                  \
    auto result_name = (expression);                                         \
    if (!result_name) [[unlikely]] {                                         \
        return ::std::unexpected(result_name.error());                      \
    }                                                                        \
    auto variable = std::move(result_name).value()

/**
 * @brief Checks a Result and extracts its value into a new variable.
 *
 * On error, immediately returns std::unexpected(error()) from the current function.
 * @param variable Name of the local variable that receives the value.
 * @param expression Expression that returns a Result-like object.
 */
#define ASYNC_TRY(variable, expression)                                       \
    ASYNC_DETAIL_TRY(variable, expression, ASYNC_DETAIL_CONCAT(_async_result_, variable))
