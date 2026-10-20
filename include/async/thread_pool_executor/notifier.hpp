//
// Created by Shagu on 13.10.2026.
//

#ifndef SHUTTLEENGINE_NOTIFIER_HPP
#define SHUTTLEENGINE_NOTIFIER_HPP

#include <atomic>

namespace shuttle::async {
    /**
     * @brief Thread-safe notification that waits on a predicate using atomic wait.
     *
     * The notification does not keep an event count: callers must check their
     * own atomic state in the predicate. The predicate may be called repeatedly;
     * notify calls increment the generation and wake waiters.
     */
    class Notifier {
    public:

        /**
         * @brief Blocks the current thread until the predicate is true.
         * @tparam Predicate Callable object that is safe to invoke repeatedly.
         * @param predicate Returns true when the wait condition is satisfied.
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

        /// Increments the generation and wakes one waiter, if any.
        void notify_one() noexcept {
            m_generation.fetch_add(1, std::memory_order_release);
            m_generation.notify_one();
        }

        /// Increments the generation and wakes all waiters.
        void notify_all() noexcept {
            m_generation.fetch_add(1, std::memory_order_release);
            m_generation.notify_all();
        }

    private:
        /// A generation change prevents a notification from being lost between the check and wait.
        std::atomic_uint64_t m_generation {0};
    };
}

#endif //SHUTTLEENGINE_NOTIFIER_HPP
