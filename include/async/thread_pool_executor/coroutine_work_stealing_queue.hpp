#pragma once

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <new> // Для корректного hardware_destructive_interference_size

namespace shuttle::async {
    /**
     * @if RU
     * @brief Ограниченная Chase–Lev deque для coroutine handles.
     *
     * Единственный владелец deque вызывает try_push/pop; другие потоки могут
     * конкурентно вызывать steal. Очередь не владеет coroutine frame, а лишь
     * передаёт handle. Емкость фиксирована, при заполнении push возвращает false.
     *
     * @tparam LogSize Двоичный логарифм ёмкости; должен быть в диапазоне [1, 30).
     * @endif
     * @if EN
     * @brief Bounded Chase–Lev deque for coroutine handles.
     *
     * Only the deque owner calls try_push/pop; other threads may concurrently
     * call steal. The queue does not own coroutine frames, it only transfers
     * handles. Capacity is fixed, and push returns false when the queue is full.
     *
     * @tparam LogSize Base-2 logarithm of the capacity; must be in [1, 30).
     * @endif
     */
    template <size_t LogSize = 12>
    class CoroutineWorkStealingQueue {
        static_assert(LogSize > 0 && LogSize < 31);

        static constexpr size_t BufferSize = size_t{1} << LogSize;
        static constexpr size_t BufferMask = BufferSize - 1;

        // Разносим атомики по разным кэш-линиям, чтобы Хозяин и Вор не блокировали друг друга на уровне L1/L2
        alignas(64) std::atomic<int64_t> _top {0};
        alignas(64) std::atomic<int64_t> _bottom {0};

        // Буфер выравниваем отдельно
        alignas(64) std::atomic<std::coroutine_handle<>> m_buffer[BufferSize];

    public:
        /// @if RU
        /// Создаёт пустую deque фиксированной ёмкости.
        /// @endif
        /// @if EN
        /// Creates an empty deque with fixed capacity.
        /// @endif
        CoroutineWorkStealingQueue() = default;
        /// @if RU
        /// Копирование очереди запрещено из-за её атомарного состояния.
        /// @endif
        /// @if EN
        /// Copying the queue is disabled because of its atomic state.
        /// @endif
        CoroutineWorkStealingQueue(const CoroutineWorkStealingQueue&) = delete;
        CoroutineWorkStealingQueue& operator=(const CoroutineWorkStealingQueue&) = delete;
        /// @if RU
        /// Перемещает внутреннее состояние; перемещать активную очередь небезопасно.
        /// @endif
        /// @if EN
        /// Moves the internal state; moving an active queue is unsafe.
        /// @endif
        CoroutineWorkStealingQueue(CoroutineWorkStealingQueue&&) = default;
        CoroutineWorkStealingQueue& operator = (CoroutineWorkStealingQueue&&) = default;

        /// @if RU
        /// @return Постоянная ёмкость очереди в элементах.
        /// @endif
        /// @if EN
        /// @return The queue's fixed capacity, in elements.
        /// @endif
        static constexpr size_t capacity() noexcept {
            return BufferSize;
        }

        /**
         * @if RU
         * @brief Приблизительно оценивает число доступных элементов.
         * @return Неотрицательная разность bottom и top на момент чтения.
         * @note При конкурентных изменениях это моментальный приблизительный снимок.
         * @endif
         * @if EN
         * @brief Approximate estimate of the number of available elements.
         * @return Non-negative difference between bottom and top at the time of reading.
         * @note Concurrent modifications may make this an instantaneous, approximate snapshot.
         * @endif
         */
        [[nodiscard]] size_t size() const noexcept {
            // Используем acquire для чтения top, чтобы гарантировать корректный размер из других потоков
            const int64_t top = _top.load(std::memory_order_acquire);
            const int64_t bottom = _bottom.load(std::memory_order_relaxed);
            return static_cast<size_t>(bottom >= top ? bottom - top : 0);
        }

        /// @if RU
        /// @return true, если моментальный снимок размера равен нулю.
        /// @endif
        /// @if EN
        /// @return true if the instantaneous size snapshot is zero.
        /// @endif
        [[nodiscard]] bool empty() const noexcept {
            return size() == 0;
        }

        /**
         * @if RU
         * @brief Добавляет handle с конца очереди (только владелец).
         * @param handle Handle корутины; владение frame не передаётся.
         * @return false, если очередь заполнена; иначе true.
         * @endif
         * @if EN
         * @brief Adds a handle at the back of the queue (owner only).
         * @param handle Coroutine handle; ownership of the frame is not transferred.
         * @return false if the queue is full; otherwise, true.
         * @endif
         */
        bool try_push(std::coroutine_handle<> handle) noexcept {
            const int64_t bottom = _bottom.load(std::memory_order_relaxed);
            // Тор читается с relaxed, так как его актуальность критична только при переполнении,
            // но для строгости алгоритма Чейза-Лева здесь лучше использовать acquire.
            const int64_t top = _top.load(std::memory_order_acquire);

            if (static_cast<size_t>(bottom - top) >= BufferSize) {
                return false; // Очередь переполнена
            }

            m_buffer[bottom & BufferMask].store(handle, std::memory_order_relaxed);

            // release гарантирует, что запись хэндла в буфер завершится ДО того, как обновится _bottom
            _bottom.store(bottom + 1, std::memory_order_release);
            return true;
        }

        /**
         * @if RU
         * @brief Извлекает самый новый handle с конца очереди (только владелец).
         * @return Handle задачи либо пустой handle, если очередь пуста/элемент
         *         уже забрал вор.
         * @endif
         * @if EN
         * @brief Removes the newest handle from the back of the queue (owner only).
         * @return Task handle, or an empty handle if the queue is empty or a thief
         *         has already taken the item.
         * @endif
         */
        std::coroutine_handle<> pop() noexcept {
            int64_t bottom = _bottom.load(std::memory_order_relaxed) - 1;
            _bottom.store(bottom, std::memory_order_relaxed);

            // КРИТИЧЕСКИЙ БАРЬЕР: Предотвращает переупорядочивание Store(_bottom) и Load(_top) процессором
            std::atomic_thread_fence(std::memory_order_seq_cst);

            int64_t top = _top.load(std::memory_order_relaxed);

            if (top <= bottom) {
                auto handle = m_buffer[bottom & BufferMask].load(std::memory_order_relaxed);
                if (top == bottom) {
                    // Последняя задача в очереди: конкурируем за нее с ворами через CAS
                    if (!_top.compare_exchange_strong(
                        top, top + 1,
                        std::memory_order_seq_cst,
                        std::memory_order_relaxed
                    )) {
                        handle = {}; // Вор оказался быстрее
                    }
                    _bottom.store(bottom + 1, std::memory_order_relaxed);
                }
                return handle;
            }

            _bottom.store(bottom + 1, std::memory_order_relaxed);
            return {};
        }

        /**
         * @if RU
         * @brief Пытается забрать самый старый handle с начала очереди.
         * @return Handle задачи либо пустой handle при пустой очереди или гонке.
         * @note Thread safety: может вызываться несколькими конкурирующими ворами.
         * @endif
         * @if EN
         * @brief Attempts to take the oldest handle from the front of the queue.
         * @return Task handle, or an empty handle if the queue is empty or a race occurs.
         * @note Thread safety: may be called by multiple competing thieves.
         * @endif
         */
        std::coroutine_handle<> steal() noexcept {
            // КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Барьер seq_cst ДОЛЖЕН стоять перед чтением _top вором,
            // чтобы синхронизировать память с методом pop() хозяина.
            int64_t top = _top.load(std::memory_order_seq_cst);
            const int64_t bottom = _bottom.load(std::memory_order_acquire);

            if (top < bottom) {
                auto handle = m_buffer[top & BufferMask].load(std::memory_order_relaxed);

                // Пытаемся атомарно сдвинуть top
                if (!_top.compare_exchange_strong(
                    top, top + 1,
                    std::memory_order_seq_cst,
                    std::memory_order_relaxed
                )) {
                    return {}; // Другой вор успел раньше
                }
                return handle;
            }

            return {};
        }
    };
}  // namespace shuttle::async
