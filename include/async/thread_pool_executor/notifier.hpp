//
// Created by Shagu on 13.10.2026.
//

#ifndef SHUTTLEENGINE_NOTIFIER_HPP
#define SHUTTLEENGINE_NOTIFIER_HPP

#include <atomic>

namespace shuttle::async {
    /**
     * @if RU
     * @brief Потокобезопасное уведомление с ожиданием предиката через atomic wait.
     *
     * Уведомление не хранит число событий: вызывающая сторона должна проверять
     * собственное атомарное состояние в predicate. Предикат может вызываться
     * несколько раз; вызовы notify увеличивают поколение и пробуждают waiter-ов.
     * @endif
     * @if EN
     * @brief Thread-safe notification that waits on a predicate using atomic wait.
     *
     * The notification does not keep an event count: callers must check their
     * own atomic state in the predicate. The predicate may be called repeatedly;
     * notify calls increment the generation and wake waiters.
     * @endif
     */
    class Notifier {
    public:

        /**
         * @if RU
         * @brief Блокирует текущий поток до истинности предиката.
         * @tparam Predicate Вызываемый объект, безопасный для повторного вызова.
         * @param predicate Возвращает true, когда условие ожидания выполнено.
         * @endif
         * @if EN
         * @brief Blocks the current thread until the predicate is true.
         * @tparam Predicate Callable object that is safe to invoke repeatedly.
         * @param predicate Returns true when the wait condition is satisfied.
         * @endif
         */
        template <typename Predicate>
        void wait(Predicate&& predicate) {
            while (!predicate()) {
                const auto observed = m_generation.load(std::memory_order_acquire);
                if (predicate()) {
                    return;
                }
                m_generation.wait(observed, std::memory_order_acquire);
            }
        }

        /// @if RU
        /// Увеличивает поколение и пробуждает одного ожидающего, если он есть.
        /// @endif
        /// @if EN
        /// Increments the generation and wakes one waiter, if any.
        /// @endif
        void notify_one() noexcept {
            m_generation.fetch_add(1, std::memory_order_release);
            m_generation.notify_one();
        }

        /// @if RU
        /// Увеличивает поколение и пробуждает всех ожидающих.
        /// @endif
        /// @if EN
        /// Increments the generation and wakes all waiters.
        /// @endif
        void notify_all() noexcept {
            m_generation.fetch_add(1, std::memory_order_release);
            m_generation.notify_all();
        }

    private:
        /// @if RU
        /// Изменение поколения предотвращает потерю уведомления между проверкой и wait.
        /// @endif
        /// @if EN
        /// A generation change prevents a notification from being lost between the check and wait.
        /// @endif
        std::atomic_uint64_t m_generation {0};
    };
}

#endif //SHUTTLEENGINE_NOTIFIER_HPP
