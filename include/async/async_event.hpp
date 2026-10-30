//
// Created by Shagu on 24.10.2026.
//

#ifndef SHUTTLE_ASYNC_ASYNC_FUTEX_HPP
#define SHUTTLE_ASYNC_ASYNC_FUTEX_HPP

#include <atomic>
#include <coroutine>
#include <cstdint>
#include "mi_allocator.hpp"

// Внешние зависимости вашего фреймворка shuttle_async
#include "concurrentqueue.h" // moodycamel::ConcurrentQueue
#include "i_executor.hpp"

namespace shuttle::async {

    class AsyncEvent final {
    public:
        AsyncEvent() = default;
        ~AsyncEvent() = default;

        // Системный примитив синхронизации строго некопируем и неперемещаем
        AsyncEvent(const AsyncEvent&) = delete;
        AsyncEvent& operator=(const AsyncEvent&) = delete;
        AsyncEvent(AsyncEvent&&) = delete;
        AsyncEvent& operator=(AsyncEvent&&) = delete;

        /**
         * @brief Регистрирует узел в стеке футекса и проверяет необходимость приостановки.
         * @return true, если нужно приостановить корутину. false, если notify() пришел раньше и спать нельзя.
         */
        bool prepareWait(WaitNode* node) noexcept {
            node->nodeState.store(NodeState::PENDING, std::memory_order_seq_cst);

            // Встаем в хвост lock-free MPMC очереди
            m_waitersQueue.enqueue(node);

            // === ПРОВЕРКА ОТМЕНЫ ПЕРЕД СОНОМ ===
            // Если флаг отмены уже взведен родителем — мы принудительно переводим
            // состояние ноды в CANCELLED, минуя фазу сна!
            if (node->parentCancelledFlag && node->parentCancelledFlag.load(std::memory_order_acquire)) [[unlikely]] {
                auto expected = NodeState::PENDING;
                if (node->nodeState.compare_exchange_strong(expected, NodeState::CANCELLED,
                        std::memory_order_acq_rel, std::memory_order_acquire))
                {
                    // Мы успешно заблокировали ноду в состоянии CANCELLED.
                    // Если параллельный поток notify() попытается её забрать, его CAS сорвется.
                    return false; // Спать нельзя, мы отменены!
                }
            }

            // Тот самый КАЗ на сон: пытаемся перевести ноду из PENDING в SUSPEND
            auto expected = NodeState::PENDING;
            if (node->nodeState.compare_exchange_strong(expected, NodeState::SUSPEND,
                    std::memory_order_acq_rel, std::memory_order_acquire))
            {
                return true; // Успех! notify() нас еще не трогал, нужно честно спать
            }

            // Если КАЗ сорвался, значит expected может быть:
            // 1. NodeState::SIGNALED — notify() успел раньше.
            // 2. NodeState::CANCELLED — параллельный поток отмены выбил нас через tryCancel().
            return false;
        }

        static bool tryCancel(WaitNode* node) noexcept {
            auto expected = NodeState::PENDING;
            // Сценарий А: Корутина еще в процессе выполнения await_suspend
            if (node->nodeState.compare_exchange_strong(expected, NodeState::CANCELLED,
                std::memory_order_acq_rel, std::memory_order_acquire))
            {
                // Мы успели перехватить ноду до того, как она уснула.
                // Когда её собственный КАЗ на сон сработает, он сорвется, и она не уснет.
                return true;
            }

            if (expected == NodeState::SUSPEND) {
                // Сценарий Б: Корутина уже честно спит в ожидании сигнала.
                if (node->nodeState.compare_exchange_strong(expected, NodeState::CANCELLED,
                    std::memory_order_acq_rel, std::memory_order_acquire))
                {
                    // Мы перевели её в CANCELLED. Теперь принудительно будим её,
                    // отправляя обратно в её экзекьютор!
                    resumeWaiter(node);
                    return true;
                }
            }

            // Если нода уже была SIGNALED или CANCELLED, отменять нечего
            return false;
        }

        // -------------------------------------------------------------------------
        // ОПЕРАЦИЯ NOTIFY_ONE (Разбудить строго одну АКТИВНУЮ корутину из очереди)
        // -------------------------------------------------------------------------
        void notifyOne() noexcept {
            WaitNode* node = nullptr;

            // Пытаемся забрать строго ОДНУ ноду из головы очереди.
            // Если очередь пуста, try_dequeue вернет false и мы сразу выходим.
            while (m_waitersQueue.try_dequeue(node)) {
                auto expected = NodeState::PENDING;

                // Пытаемся атомарно перевести ноду в SIGNALED
                if (node->nodeState.compare_exchange_strong(expected, NodeState::SIGNALED,
                        std::memory_order_acq_rel, std::memory_order_acquire))
                {
                    // Случай А: Корутина в процессе prepare_wait. Мы выставили SIGNALED,
                    // её КАЗ на сон сорвется, и она полетит дальше. Цель достигнута, выходим!
                    return;
                }
                if (expected == NodeState::SUSPEND) {
                    // Случай Б: Корутина уже честно спала. Пытаемся перевести в SIGNALED.
                    // Если мы выиграли гонку у tryCancel — будим её через экзекьютор.
                    if (node->nodeState.compare_exchange_strong(expected, NodeState::SIGNALED,
                            std::memory_order_acq_rel, std::memory_order_acquire))
                    {
                        resumeWaiter(node);
                        return;
                    }
                }
                // Если expected оказался равен чему-то другому (например, внешняя отмена),
                // мы просто игнорируем эту "мертвую" ноду и идем на следующий круг try_dequeue.
            }
        }

        // -------------------------------------------------------------------------
        // ОПЕРАЦИЯ NOTIFY_ALL (Мгновенно разбудить ВСЕХ активных спящих)
        // -------------------------------------------------------------------------
        void notifyAll() noexcept {
            WaitNode* node = nullptr;

            // Выгребаем абсолютно ВСЕ ноды из lock-free очереди
            while (m_waitersQueue.try_dequeue(node)) {
                auto expected = NodeState::PENDING;

                // 1. Попытка перехватить ноду на взлете (до засыпания)
                if (node->nodeState.compare_exchange_strong(expected, NodeState::SIGNALED,
                        std::memory_order_acq_rel, std::memory_order_acquire))
                {
                    // Успех! Корутина в prepareWait увидит SIGNALED, КАЗ на сон сорвется,
                    // и она пойдет дальше без приостановки.
                    continue;
                }

                // 2. Если она уже успела уснуть (SUSPEND)
                if (expected == NodeState::SUSPEND) {
                    // КРИТИЧНО ДЛЯ БЕЗОПАСНОСТИ: Пытаемся атомарно перевести из SUSPEND в SIGNALED.
                    // Если параллельный поток отмены успел перевести её в CANCELLED — наш CAS сорвется.
                    if (node->nodeState.compare_exchange_strong(expected, NodeState::SIGNALED,
                            std::memory_order_acq_rel, std::memory_order_acquire))
                    {
                        // Мы выиграли гонку у отмены! Теперь только МЫ имеем право её разбудить.
                        resumeWaiter(node);
                    }
                }
                // 3. Если expected == NodeState::CANCELLED или NodeState::SIGNALED:
                // Нода уже обработана потоком отмены или другим вызовом.
                // Мы просто бесшумно её пропускаем и идем к следующему элементу очереди.
            }
        }


    private:
        alignas(64) moodycamel::ConcurrentQueue<WaitNode*> m_waitersQueue{};

        // Единственная точка глобальной синхронизации — число РЕАЛЬНО спящих корутин.
        alignas(64) std::atomic<uint32_t> m_waiterCount{0};

        // Хелпер возобновления корутины и очистки ресурсов
        static void resumeWaiter(WaitNode* node) noexcept {
            auto h = node->handle;
            auto exec = node->executor;

            if (exec) [[likely]] {
                if (!exec->execute(h)) {
                    h.resume();
                }
            } else {
                h.resume();
            }
        }
    };
} // namespace shuttle::async

#endif //SHUTTLE_ASYNC_ASYNC_FUTEX_HPP