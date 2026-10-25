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

    class AsyncFutex final {
    public:
        AsyncFutex() = default;
        ~AsyncFutex() = default;

        // Системный примитив синхронизации строго некопируем и неперемещаем
        AsyncFutex(const AsyncFutex&) = delete;
        AsyncFutex& operator=(const AsyncFutex&) = delete;
        AsyncFutex(AsyncFutex&&) = delete;
        AsyncFutex& operator=(AsyncFutex&&) = delete;

        // -------------------------------------------------------------------------
        // УЛЬТИМАТИВНЫЙ ШАБЛОННЫЙ АВЕЙТЕР (Реализация по принципу std::atomic::wait)
        // -------------------------------------------------------------------------
        template <typename T>
        struct [[nodiscard]] FutexWaitValueAwaiter {
            AsyncFutex&           futex;
            const std::atomic<T>& atomicVariable;
            T                     oldValue; // Значение, при котором корутина ОБЯЗАНА спать
            IExecutor*            executor;

            // 1. БЫСТРЫЙ ПУТЬ (Юзерспейс):
            // Если текущее значение в атомике УЖЕ изменилось (не равно oldValue),
            // метод возвращает true. Корутина пролетает дальше без выделения памяти.
            [[nodiscard]] bool await_ready() const noexcept {
                return atomicVariable.load(std::memory_order_acquire) != oldValue;
            }

            // 2. МЕДЛЕННЫЙ ПУТЬ (Уход в сон):
            std::coroutine_handle<> await_suspend(std::coroutine_handle<> handle) noexcept {
                // Аллокация ноды через lock-free mimalloc
                WaitNode* node = MiAllocator<WaitNode>{}.allocate(1);
                node->handle = handle;
                node->executor = executor;

                // Встаем в хвост lock-free очереди Кэмерона
                futex.m_waitersQueue.enqueue(node);

                // Атомарно объявляем всей системе, что корутина легла спать
                futex.m_waiterCount.fetch_add(1, std::memory_order_release);

                // КРИТИЧЕСКИЙ РУБЕЖ (Защита от упущенного сигнала + Отложенное удаление):
                if (atomicVariable.load(std::memory_order_acquire) != oldValue) {

                    // Пытаемся атомарно пометить НАШУ ноду как КАНСЕЛНУТУЮ
                    auto expected = NodeState::SLEEPING;
                    if (node->nodeState.compare_exchange_strong(expected, NodeState::CANCELLED,
                            std::memory_order_acq_rel, std::memory_order_acquire))
                    {
                        // Успех! Поток notify еще не успел дойти до нашей ноды.
                        // Откатываем глобальный счетчик спящих назад
                        futex.m_waiterCount.fetch_sub(1, std::memory_order_release);

                        // Сами чистим свою память в текущем потоке
                        MiAllocator<WaitNode>{}.deallocate(node);

                        // Возвращаем false -> корутина НЕ засыпает и идет дальше
                        return handle;
                    }

                    // Если КАЗ сорвался, значит expected стал равен NODE_SIGNALED.
                    // Это означает, что параллельный поток notify ОДНОВРЕМЕННО выгреб
                    // нашу ноду из головы очереди и прямо сейчас вызывает для нее resume()!
                    // Мы не имеем права трогать ноду и возвращаем true — пусть нас будит сигнал.
                }

                return IExecutor::getCurrentDispatcherCoroutine(); // Значение стабильно, корутина честно спит в пуле
            }

            // ReSharper disable once CppMemberFunctionMayBeStatic
            void await_resume() const noexcept {}
        };

        // Фабричный метод-генератор: co_await futex.wait(my_atomic, old_value);
        template <typename T>
        [[nodiscard]] auto wait(const std::atomic<T>& atomicVar, T oldValue, IExecutor* executor = nullptr) noexcept {
            return FutexWaitValueAwaiter<T>{*this, atomicVar, oldValue, executor};
        }

        // -------------------------------------------------------------------------
        // ОПЕРАЦИЯ NOTIFY_ONE (Разбудить строго одну АКТИВНУЮ корутину из очереди)
        // -------------------------------------------------------------------------
        void notifyOne() noexcept {
            uint32_t count = m_waiterCount.load(std::memory_order_acquire);

            while (count > 0) {
                // Пытаемся атомарно забрать ПРАВО на пробуждение строго одной корутины
                if (m_waiterCount.compare_exchange_weak(count, count - 1,
                        std::memory_order_acq_rel, std::memory_order_acquire))
                {
                    WaitNode* node = nullptr;
                    // Спинлок ожидания долетания указателя по шине данных в moodycamel
                    while (!m_waitersQueue.try_dequeue(node)) {
                        #if defined(_MSC_VER)
                        _mm_pause();
                        #else
                        __builtin_ia32_pause();
                        #endif
                    }

                    // Пытаемся атомарно захватить ноду (перевести из SLEEPING в SIGNALED)
                    auto expected = NodeState::SLEEPING;
                    if (node->nodeState.compare_exchange_strong(expected, NodeState::SIGNALED,
                            std::memory_order_acq_rel, std::memory_order_acquire))
                    {
                        // Нода живая, корутина реально ждет — будим её!
                        resumeWaiter(node);
                        return;
                    }

                    // Если там было NODE_CANCELLED (корутина сама отменила сон на КРИТИЧЕСКОМ РУБЕЖЕ):
                    // Мы просто утилизируем этот мертвый груз из памяти кучи
                    MiAllocator<WaitNode>{}.deallocate(node);

                    // Перезагружаем счетчик и ИДЕМ НА СЛЕДУЮЩИЙ КРУГ, чтобы разбудить РЕАЛЬНОГО спящего
                    count = m_waiterCount.load(std::memory_order_acquire);
                    if (count == 0) return;
                }
            }
        }

        // -------------------------------------------------------------------------
        // ОПЕРАЦИЯ NOTIFY_ALL (Мгновенно разбудить ВСЕХ активных спящих)
        // -------------------------------------------------------------------------
        void notifyAll() noexcept {
            // Атомарно забираем ТОЧНЫЙ снимок количества спящих, обнуляя счетчик за один такт!
            uint32_t toWake = m_waiterCount.exchange(0, std::memory_order_acq_rel);

            // Спокойно выкачиваем из очереди именно это фиксированное число нод
            for (uint32_t i = 0; i < toWake; ++i) {
                WaitNode* node = nullptr;
                while (!m_waitersQueue.try_dequeue(node)) {
                    #if defined(_MSC_VER)
                    _mm_pause();
                    #else
                    __builtin_ia32_pause();
                    #endif
                }

                // Пытаемся атомарно перевести ноду из SLEEPING в SIGNALED
                auto expected = NodeState::SLEEPING;
                if (node->nodeState.compare_exchange_strong(expected, NodeState::SIGNALED,
                        std::memory_order_acq_rel, std::memory_order_acquire))
                {
                    // Нода валидна — будим корутину
                    resumeWaiter(node);
                } else {
                    // Нода была отменена корутиной самостоятельно — просто чистим память кучи
                    MiAllocator<WaitNode>{}.deallocate(node);
                }
            }
        }

    private:

        // Константы состояний индивидуального жизненного цикла ноды
        enum class NodeState : uint64_t {
            SLEEPING, // Корутина честно спит
            SIGNALED, // Нода захвачена потоком notify
            CANCELLED // Корутина сама отменила сон
        };

        // Легковесный контекст ожидания
        struct WaitNode {
            std::coroutine_handle<> handle;
            IExecutor*              executor;
            // Индивидуальный атомарный замок ноды для предотвращения гонок в FIFO-очереди
            std::atomic<NodeState>   nodeState{ NodeState::SLEEPING };
        };

        // Единственная точка глобальной синхронизации — число РЕАЛЬНО спящих корутин.
        alignas(64) std::atomic<uint32_t> m_waiterCount{0};

        // Потокобезопасный транспорт для передачи указателей из кучи между ядрами CPU
        moodycamel::ConcurrentQueue<WaitNode*> m_waitersQueue;

        // Хелпер возобновления корутины и очистки ресурсов
        static void resumeWaiter(WaitNode* node) noexcept {
            auto h = node->handle;
            auto exec = node->executor;

            // Освобождаем память ноды через mimalloc строго в потоке-будителе
            MiAllocator<WaitNode>{}.deallocate(node);

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
