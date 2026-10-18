#pragma once

#include <atomic>
#include <concepts>
#include <coroutine>
#include <exception>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include <async/async_result.hpp>
#include <async/error.hpp>
#include <async/i_executor.hpp>
#include <async/mi_allocator.hpp>

namespace shuttle::async {

    /**
     * @if RU
     * @brief Фазы жизненного цикла общего состояния задачи.
     *
     * Переходы выполняются атомарно: Pending может перейти в Suspended либо
     * Signaled, а Suspended — в Signaled. Повторное завершение не сигнализирует
     * waiter повторно.
     * @endif
     * @if EN
     * @brief Lifecycle phases of a task's shared state.
     *
     * Transitions are atomic: Pending may transition to Suspended or Signaled,
     * and Suspended may transition to Signaled. Repeated completion does not
     * signal a waiter more than once.
     * @endif
     */
    enum class AsyncTaskState : uint32_t {
        /// @if RU
        /// Состояние создано, завершение ещё не опубликовано.
        /// @endif
        /// @if EN
        /// State has been created; completion has not yet been published.
        /// @endif
        Pending = 0,
        /// @if RU
        /// Зарегистрирован единственный ожидающий coroutine handle.
        /// @endif
        /// @if EN
        /// The single waiting coroutine handle has been registered.
        /// @endif
        Suspended = 1,
        /// @if RU
        /// Завершение или отмена опубликованы.
        /// @endif
        /// @if EN
        /// Completion or cancellation has been published.
        /// @endif
        Signaled = 2
    };

    template <typename T> class AsyncSharedState;
    template <typename T> class AsyncFuture;
    template <typename T> class AsyncPromise;
    template <typename T> class AsyncStateFactory;
    template <typename T> struct AsyncStatePair;

    /**
     * @if RU
     * @brief Внутреннее общее состояние producer/consumer асинхронной задачи.
     *
     * Создаётся только AsyncStateFactory внутри allocator-aware блока и имеет
     * intrusive reference counting. AsyncPromise, AsyncFuture и их awaiter-ы
     * держат отдельные ссылки. Результат T размещается inline посредством
     * placement new, а пользовательский allocator выделяет единый блок
     * AllocationBlock; последний release вызывает сохранённый deleter для
     * разрушения блока и возврата памяти allocator-у.
     *
     * Разрешено единственное значение и единственный consumer. Для `void`
     * value storage не содержит T. Нельзя уничтожать состояние до завершения
     * всех операций, которые используют его внутренний указатель.
     *
     * @tparam T Тип результата; может быть void.
     * @endif
     * @if EN
     * @brief Internal shared producer/consumer state for an asynchronous task.
     *
     * Created only by AsyncStateFactory inside an allocator-aware block, this
     * state uses intrusive reference counting. AsyncPromise, AsyncFuture, and
     * their awaiters each hold separate references. The result T is stored
     * inline using placement new; the user allocator allocates one AllocationBlock.
     * The final release invokes the saved deleter to destroy the block and return
     * its memory to the allocator.
     *
     * Exactly one value and one consumer are allowed. For `void`, the value
     * storage contains no T. Do not destroy the state while any operation still
     * uses its internal pointer.
     *
     * @tparam T Result type; may be void.
     * @endif
     */
    template <typename T>
    class AsyncSharedState final {
    public:
        /// @if RU
        /// Уничтожает созданное значение (если оно есть); освобождение блока
        /// выполняет последний release().
        /// @endif
        /// @if EN
        /// Destroys the constructed value, if any; the final release() frees the block.
        /// @endif
        ~AsyncSharedState() noexcept {
            if constexpr (!std::same_as<T, void>) {
                if (isSignaled() && hasValue()) {
                    reinterpret_cast<T*>(m_valueBuf)->~T();
                }
            }
        }

        /// @if RU
        /// Состояние не копируется: его lifetime управляется счётчиком ссылок.
        /// @endif
        /// @if EN
        /// State is non-copyable; its lifetime is managed by the reference count.
        /// @endif
        AsyncSharedState(const AsyncSharedState&) = delete;
        AsyncSharedState& operator=(const AsyncSharedState&) = delete;

        /// @if RU
        /// Добавляет владеющую ссылку; вызывающий обязан позже вызвать release().
        /// @endif
        /// @if EN
        /// Adds an owning reference; the caller must later call release().
        /// @endif
        void retain() noexcept {
            m_referenceCount.fetch_add(1, std::memory_order_relaxed);
        }

        /// @if RU
        /// Удаляет владеющую ссылку и при последней ссылке уничтожает allocator block.
        /// @endif
        /// @if EN
        /// Removes an owning reference and destroys the allocator block on the final reference.
        /// @endif
        void release() noexcept {
            if (m_referenceCount.fetch_sub(1, std::memory_order_acq_rel) == 1) [[unlikely]] {
                m_destroyAllocation(this);
            }
        }

        /// @if RU
        /// @return true, если состояние завершено или отменено и сигнал опубликован.
        /// @endif
        /// @if EN
        /// @return true if completion or cancellation has been signaled.
        /// @endif
        [[nodiscard]] bool isSignaled() const noexcept {
            return m_state.load(std::memory_order_acquire) == AsyncTaskState::Signaled;
        }

        /// @if RU
        /// Атомарно резервирует единственное потребление результата.
        /// @return true только для первого вызова.
        /// @endif
        /// @if EN
        /// Atomically claims the single consumption of the result.
        /// @return true only for the first call.
        /// @endif
        [[nodiscard]] bool claimConsumer() noexcept {
            bool expected = false;
            return m_consumerClaimed.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel, std::memory_order_relaxed);
        }

        /// @if RU
        /// Атомарно резервирует единственную запись значения.
        /// @return true только для первого вызова.
        /// @endif
        /// @if EN
        /// Atomically claims the single write of a value.
        /// @return true only for the first call.
        /// @endif
        [[nodiscard]] bool claimValue() noexcept {
            bool expected = false;
            return m_valueSet.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel, std::memory_order_relaxed);
        }

        /// @if RU
        /// @return true, если producer уже разместил значение в inline storage.
        /// @endif
        /// @if EN
        /// @return true if the producer has placed a value in inline storage.
        /// @endif
        [[nodiscard]] bool hasValue() const noexcept {
            return m_valueSet.load(std::memory_order_acquire);
        }

        /// @if RU
        /// Атомарно помечает запуск начатым.
        /// @return true, если этот вызов первым зарезервировал старт.
        /// @endif
        /// @if EN
        /// Atomically marks the task as started.
        /// @return true if this call was the first to claim the start.
        /// @endif
        [[nodiscard]] bool beginStart() noexcept {
            bool expected = false;
            return m_started.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel, std::memory_order_acquire);
        }

        /// @if RU
        /// Снимает отметку запуска, например после отказа executor-а.
        /// @endif
        /// @if EN
        /// Clears the started flag, for example after an executor rejects the task.
        /// @endif
        void resetStarted() noexcept {
            m_started.store(false, std::memory_order_release);
        }

        /// @if RU
        /// Отмечает задачу запущенной без проверки предыдущего состояния.
        /// @endif
        /// @if EN
        /// Marks the task as started without checking its previous state.
        /// @endif
        void markStarted() noexcept {
            m_started.store(true, std::memory_order_release);
        }

        /// @if RU
        /// Устанавливает флаг отмены; не публикует завершение само по себе.
        /// @endif
        /// @if EN
        /// Sets the cancellation flag; this alone does not publish completion.
        /// @endif
        void cancel() noexcept {
            m_isCanceled.store(true, std::memory_order_release);
        }

        /// @if RU
        /// Отмечает незавершённое состояние отменённым и публикует завершение.
        /// Уже завершённое состояние не меняется.
        /// @endif
        /// @if EN
        /// Marks an incomplete state as cancelled and publishes completion.
        /// An already completed state is unchanged.
        /// @endif
        void abandon() noexcept {
            if (!isSignaled()) {
                m_isCanceled.store(true, std::memory_order_release);
                static_cast<void>(signalReady());
            }
        }

        /// @if RU
        /// @return true, если установлен флаг отмены.
        /// @endif
        /// @if EN
        /// @return true if the cancellation flag is set.
        /// @endif
        [[nodiscard]] bool isCanceled() const noexcept {
            return m_isCanceled.load(std::memory_order_acquire);
        }

        /// @if RU
        /// Возвращает адрес атомарного флага отмены для передачи awaiter-ам.
        /// Указатель не владеющий и действителен только пока живо состояние.
        /// @endif
        /// @if EN
        /// Returns the address of the atomic cancellation flag for awaiters.
        /// The pointer is non-owning and is valid only while the state is alive.
        /// @endif
        [[nodiscard]] std::atomic<bool>* cancellationFlag() noexcept {
            return &m_isCanceled;
        }

        /**
         * @if RU
         * @brief Регистрирует единственного waiter-а, если состояние ещё Pending.
         * @param handle Coroutine handle, который будет передан executor-у при сигнале.
         * @param executor Не владеющий executor для возобновления waiter-а.
         * @return true, если состояние стало Suspended; иначе waiter не зарегистрирован.
         * @endif
         * @if EN
         * @brief Registers the sole waiter if the state is still Pending.
         * @param handle Coroutine handle to pass to the executor when signaled.
         * @param executor Non-owning executor used to resume the waiter.
         * @return true if the state became Suspended; otherwise, no waiter was registered.
         * @endif
         */
        [[nodiscard]] bool tryRegisterWaiter(
            std::coroutine_handle<> handle,
            IExecutor* executor) noexcept
        {
            m_handle = handle;
            m_executor = executor;

            AsyncTaskState expected = AsyncTaskState::Pending;
            return m_state.compare_exchange_strong(
                expected, AsyncTaskState::Suspended,
                std::memory_order_release, std::memory_order_relaxed);
        }

        /**
         * @if RU
         * @brief Публикует завершение и при необходимости возобновляет waiter-а.
         * @return true, если waiter был зарегистрирован; возобновление могло быть
         *         отправлено executor-у либо выполнено синхронно после ошибки.
         * @note При ошибке execute() waiter возобновляется непосредственно на
         *       вызывающем потоке. Повторный сигнал не выполняет возобновление.
         * @endif
         * @if EN
         * @brief Publishes completion and resumes the waiter, if needed.
         * @return true if a waiter was registered; resumption may have been
         *         submitted to the executor or performed synchronously after an error.
         * @note If execute() fails, the waiter resumes directly on the calling
         *       thread. A repeated signal does not resume it again.
         * @endif
         */
        [[nodiscard]] bool signalReady() noexcept {
            AsyncTaskState expected = AsyncTaskState::Pending;
            if (m_state.compare_exchange_strong(
                    expected, AsyncTaskState::Signaled,
                    std::memory_order_release, std::memory_order_relaxed)) {
                return false;
            }

            expected = AsyncTaskState::Suspended;
            if (!m_state.compare_exchange_strong(
                    expected, AsyncTaskState::Signaled,
                    std::memory_order_acq_rel, std::memory_order_acquire)) {
                return false;
            }

            if (m_executor && m_handle) [[likely]] {
                auto resumeResult = m_executor->execute(m_handle);
                if (!resumeResult) [[unlikely]] {
                    m_handle.resume();
                }
                return true;
            }
            return false;
        }

        /// @if RU
        /// Возвращает ссылку на результат, перемещая его из storage.
        /// Вызывать можно только после успешного завершения и единственного claimConsumer().
        /// @endif
        /// @if EN
        /// Returns a reference to the result, moving it from storage.
        /// Call only after successful completion and the sole claimConsumer().
        /// @endif
        template <typename U = T>
        [[nodiscard]] U&& extractValue() noexcept requires (!std::same_as<U, void>) {
            return std::move(*reinterpret_cast<U*>(m_valueBuf));
        }

        /// @if RU
        /// Получает storage для placement-new результата.
        /// @return Адрес inline буфера; вызывающая сторона отвечает за однократную конструкцию.
        /// @endif
        /// @if EN
        /// Gets storage for placement-new construction of the result.
        /// @return Address of the inline buffer; the caller is responsible for constructing exactly once.
        /// @endif
        template <typename U = T>
        [[nodiscard]] void* valueBuffer() noexcept requires (!std::same_as<U, void>) {
            return static_cast<void*>(m_valueBuf);
        }

    private:
        template <typename> friend class AsyncStateFactory;

        using DestroyAllocation = void(*)(AsyncSharedState*) noexcept;
        using ValueStorage = std::conditional_t<std::same_as<T, void>, std::max_align_t, T>;

        /**
         * @if RU
         * @brief Инициализирует внутренний state; доступен только фабрике.
         * @param references Начальный refcount владельцев (Promise и Future).
         * @param started Начальное состояние флага запуска.
         * @param allocation Начало allocator-owned блока.
         * @param destroyAllocation Callback освобождения всего блока.
         * @endif
         * @if EN
         * @brief Initializes internal state; accessible only to the factory.
         * @param references Initial reference count for the owners (Promise and Future).
         * @param started Initial value of the started flag.
         * @param allocation Beginning of the allocator-owned block.
         * @param destroyAllocation Callback that frees the entire block.
         * @endif
         */
        AsyncSharedState(
            uint32_t references,
            bool started,
            void* allocation,
            DestroyAllocation destroyAllocation) noexcept
            : m_referenceCount(references),
              m_started(started),
              m_state(AsyncTaskState::Pending),
              m_handle(nullptr),
              m_executor(nullptr),
              m_allocation(allocation),
              m_destroyAllocation(destroyAllocation) {}

        /**
         * @if RU
         * @brief Устанавливает данные callback-а освобождения allocator block.
         * @param allocation Начальный адрес блока, содержащего state.
         * @param destroyAllocation Разрушитель блока; вызывается последней ссылкой.
         * @endif
         * @if EN
         * @brief Sets the allocator-block deallocation callback data.
         * @param allocation Starting address of the block containing the state.
         * @param destroyAllocation Block destructor, called by the final reference.
         * @endif
         */
        void setAllocation(void* allocation, DestroyAllocation destroyAllocation) noexcept {
            m_allocation = allocation;
            m_destroyAllocation = destroyAllocation;
        }

        /// @if RU
        /// Возвращает начало allocator-owned блока, используемое при последнем release.
        /// @endif
        /// @if EN
        /// Returns the beginning of the allocator-owned block used by the final release.
        /// @endif
        [[nodiscard]] void* allocation() const noexcept {
            return m_allocation;
        }

        /// @if RU
        /// Число владеющих ссылок; начальное значение равно двум (Promise и Future).
        /// @endif
        /// @if EN
        /// Number of owning references; initially two (Promise and Future).
        /// @endif
        std::atomic<uint32_t> m_referenceCount;
        /// @if RU
        /// Защищает единственный запуск задачи.
        /// @endif
        /// @if EN
        /// Guards the task's one-time start.
        /// @endif
        std::atomic<bool> m_started;
        /// @if RU
        /// Кооперативный флаг отмены; его установка отдельно от сигнализации.
        /// @endif
        /// @if EN
        /// Cooperative cancellation flag; setting it is separate from signaling.
        /// @endif
        std::atomic<bool> m_isCanceled{false};
        /// @if RU
        /// Резервируется до placement construction результата.
        /// @endif
        /// @if EN
        /// Claimed before placement-constructing the result.
        /// @endif
        std::atomic<bool> m_valueSet{false};
        /// @if RU
        /// Атомарная резервация единственного consumer-а.
        /// @endif
        /// @if EN
        /// Atomic claim for the sole consumer.
        /// @endif
        alignas(64) std::atomic<bool> m_consumerClaimed{false};
        /// State machine Pending -> Suspended/Signaled.
        alignas(64) std::atomic<AsyncTaskState> m_state;
        /// @if RU
        /// Единственный зарегистрированный waiter; handle не владеющий.
        /// @endif
        /// @if EN
        /// Sole registered waiter; the handle is non-owning.
        /// @endif
        alignas(64) std::coroutine_handle<> m_handle;
        /// @if RU
        /// Не владеющий executor waiter-а, обязанный пережить ожидание.
        /// @endif
        /// @if EN
        /// Non-owning executor for the waiter; it must outlive the wait.
        /// @endif
        IExecutor* m_executor;
        /// @if RU
        /// Inline storage результата; T существует только после claimValue/placement new.
        /// @endif
        /// @if EN
        /// Inline result storage; T exists only after claimValue/placement new.
        /// @endif
        alignas(64) char m_valueBuf[std::same_as<T, void> ? 1 : sizeof(ValueStorage)]{};
        /// @if RU
        /// Начало AllocationBlock с сохранённым rebound allocator-ом.
        /// @endif
        /// @if EN
        /// Start of the AllocationBlock containing the saved rebound allocator.
        /// @endif
        void* m_allocation;
        /// @if RU
        /// Callback разрушает AllocationBlock и освобождает его тем же allocator-ом.
        /// @endif
        /// @if EN
        /// Callback that destroys the AllocationBlock and frees it with the same allocator.
        /// @endif
        DestroyAllocation m_destroyAllocation;
    };

    /**
     * @if RU
     * @brief Move-only consumer-владелец общего состояния.
     *
     * Future владеет одной intrusive-ссылкой. Подготовленные awaitable и awaiter
     * удерживают собственные ссылки, поэтому они могут пережить перемещение или
     * уничтожение Future. Один результат можно потребить только одним awaiter-ом.
     * Пустой объект безопасно перемещать/уничтожать; его проверка ожидания
     * трактуется как отменённый результат.
     *
     * @tparam T Тип результата или void.
     * @endif
     * @if EN
     * @brief Move-only consumer owner of shared state.
     *
     * The future owns one intrusive reference. Prepared awaitables and awaiters
     * hold their own references and can therefore outlive a moved-from or
     * destroyed future. Only one awaiter may consume a result. An empty object
     * can be moved or destroyed safely; awaiting it is treated as a cancelled result.
     *
     * @tparam T Result type or void.
     * @endif
     */
    template <typename T>
    class AsyncFuture final {
    public:
        /// @if RU
        /// Создаёт пустой Future без состояния.
        /// @endif
        /// @if EN
        /// Creates an empty Future with no state.
        /// @endif
        AsyncFuture() noexcept = default;

        /// @if RU
        /// Освобождает ссылку Future, если она есть.
        /// @endif
        /// @if EN
        /// Releases the Future's reference, if present.
        /// @endif
        ~AsyncFuture() noexcept {
            if (m_state) {
                m_state->release();
            }
        }

        /// @if RU
        /// Копирование запрещено, чтобы consumer-ссылка имела единственного владельца.
        /// @endif
        /// @if EN
        /// Copying is disabled so the consumer reference has a single owner.
        /// @endif
        AsyncFuture(const AsyncFuture&) = delete;
        AsyncFuture& operator=(const AsyncFuture&) = delete;

        /// @if RU
        /// Передаёт владение ссылкой, оставляя источник пустым.
        /// @param other Источник, который после перемещения теряет состояние.
        /// @endif
        /// @if EN
        /// Transfers ownership of the reference, leaving the source empty.
        /// @param other Source, which has no state after the move.
        /// @endif
        AsyncFuture(AsyncFuture&& other) noexcept
            : m_state(std::exchange(other.m_state, nullptr)) {}

        /// @if RU
        /// Освобождает прежнюю ссылку и принимает ссылку источника.
        /// @param other Источник владения; самоприсваивание безопасно.
        /// @return Ссылка на этот Future.
        /// @endif
        /// @if EN
        /// Releases the previous reference and takes the source's reference.
        /// @param other Ownership source; self-assignment is safe.
        /// @return Reference to this Future.
        /// @endif
        AsyncFuture& operator=(AsyncFuture&& other) noexcept {
            if (this != &other) {
                if (m_state) {
                    m_state->release();
                }
                m_state = std::exchange(other.m_state, nullptr);
            }
            return *this;
        }

        /**
         * @if RU
         * @brief Awaiter, который регистрирует coroutine как waiter состояния.
         *
         * Хранит собственную ссылку на state. executor и cancellation flag —
         * не владеющие указатели, их lifetime должен покрывать ожидание.
         * @endif
         * @if EN
         * @brief Awaiter that registers a coroutine as a state waiter.
         *
         * Holds its own reference to the state. The executor and cancellation
         * flag are non-owning pointers and must remain alive throughout the wait.
         * @endif
         */
        struct FutureAwaiter {
            /// @if RU
            /// Состояние, удерживаемое дополнительной ссылкой данного awaiter-а.
            /// @endif
            /// @if EN
            /// State held by this awaiter's additional reference.
            /// @endif
            AsyncSharedState<T>* m_state{nullptr};
            /// @if RU
            /// Не владеющий executor для возобновления ожидающей coroutine.
            /// @endif
            /// @if EN
            /// Non-owning executor used to resume the waiting coroutine.
            /// @endif
            IExecutor* m_executor{nullptr};
            /// @if RU
            /// Не владеющий флаг отмены родительской задачи.
            /// @endif
            /// @if EN
            /// Non-owning cancellation flag of the parent task.
            /// @endif
            std::atomic<bool>* parentCancelledFlag{nullptr};

            /// @if RU
            /// Удерживает state и сохраняет executor/cancellation flag.
            /// @endif
            /// @if EN
            /// Retains the state and stores the executor/cancellation flag.
            /// @endif
            FutureAwaiter(
                AsyncSharedState<T>* state,
                IExecutor* executor,
                std::atomic<bool>* cancellationFlag) noexcept
                : m_state(state), m_executor(executor), parentCancelledFlag(cancellationFlag) {
                if (m_state) {
                    m_state->retain();
                }
            }

            /// @if RU
            /// Освобождает принадлежащую awaiter-у ссылку на state.
            /// @endif
            /// @if EN
            /// Releases the state reference owned by the awaiter.
            /// @endif
            ~FutureAwaiter() noexcept {
                if (m_state) {
                    m_state->release();
                }
            }

            /// @if RU
            /// Awaiter не копируется, чтобы исключить неоднозначное владение ссылкой.
            /// @endif
            /// @if EN
            /// Awaiter is non-copyable to avoid ambiguous reference ownership.
            /// @endif
            FutureAwaiter(const FutureAwaiter&) = delete;
            FutureAwaiter& operator=(const FutureAwaiter&) = delete;

            /// @if RU
            /// Перемещает ссылку и параметры ожидания.
            /// @endif
            /// @if EN
            /// Moves the reference and wait parameters.
            /// @endif
            FutureAwaiter(FutureAwaiter&& other) noexcept
                : m_state(std::exchange(other.m_state, nullptr)),
                  m_executor(other.m_executor),
                  parentCancelledFlag(other.parentCancelledFlag) {}

            /// @if RU
            /// Освобождает прежнюю ссылку и принимает awaiter источника.
            /// @endif
            /// @if EN
            /// Releases the previous reference and takes the source awaiter's reference.
            /// @endif
            FutureAwaiter& operator=(FutureAwaiter&& other) noexcept {
                if (this != &other) {
                    if (m_state) {
                        m_state->release();
                    }
                    m_state = std::exchange(other.m_state, nullptr);
                    m_executor = other.m_executor;
                    parentCancelledFlag = other.parentCancelledFlag;
                }
                return *this;
            }

            /// @if RU
            /// @return true для пустого Future либо уже сигнализированного состояния.
            /// @endif
            /// @if EN
            /// @return true for an empty Future or an already signaled state.
            /// @endif
            [[nodiscard]] bool await_ready() const noexcept {
                return !m_state || m_state->isSignaled();
            }

            /**
             * @if RU
             * @brief Регистрирует waiter или продолжает немедленно.
             * @param handle Ожидающая корутина.
             * @return Dispatcher handle, если ожидание зарегистрировано; иначе
             *         исходный handle для немедленного продолжения.
             * @endif
             * @if EN
             * @brief Registers the waiter or continues immediately.
             * @param handle Coroutine that is waiting.
             * @return Dispatcher handle if the wait was registered; otherwise,
             *         the original handle for immediate continuation.
             * @endif
             */
            std::coroutine_handle<> await_suspend(std::coroutine_handle<> handle) noexcept {
                if (parentCancelledFlag
                    && parentCancelledFlag->load(std::memory_order_acquire)) {
                    return handle;
                }
                if (m_state && m_state->tryRegisterWaiter(handle, m_executor)) {
                    auto const dispatcher = IExecutor::getCurrentDispatcherCoroutine();
                    return dispatcher ? dispatcher : std::noop_coroutine();
                }
                return handle;
            }

            /**
             * @if RU
             * @brief Возвращает AsyncResult после пробуждения.
             * @return Для T — перемещённый результат или значение T{} при отмене;
             *         для void — только флаг отмены.
             * @note Для отменённого T требуется default-initializable T.
             * @endif
             * @if EN
             * @brief Returns AsyncResult after wakeup.
             * @return For T, the moved result or a value-initialized T{} on
             *         cancellation; for void, only the cancellation flag.
             * @note A cancelled T requires T to be default-initializable.
             * @endif
             */
            [[nodiscard]] auto await_resume() noexcept {
                const bool cancelled =
                    (parentCancelledFlag
                        && parentCancelledFlag->load(std::memory_order_acquire))
                    || !m_state
                    || m_state->isCanceled();

                if constexpr (std::same_as<T, void>) {
                    return AsyncResult<void>{.isCancelled = cancelled};
                } else {
                    if (cancelled) [[unlikely]] {
                        return AsyncResult<T>{.value = T{}, .isCancelled = true};
                    }
                    return AsyncResult<T>{
                        .value = m_state->extractValue(), .isCancelled = false};
                }
            }
        };

        /**
         * @if RU
         * @brief Объект awaitable, позволяющий одноразово получить FutureAwaiter.
         *
         * Удерживает собственную ссылку на state до разрушения; только один
         * awaiter может быть получен из awaitable/Future для данного состояния.
         * @endif
         * @if EN
         * @brief Awaitable object that provides one-time access to a FutureAwaiter.
         *
         * Holds its own reference to the state until destruction; only one
         * awaiter may be obtained from the awaitable/Future for this state.
         * @endif
         */
        struct FutureAwaitable {
            /// @if RU
            /// Состояние, удерживаемое дополнительной ссылкой awaitable.
            /// @endif
            /// @if EN
            /// State held by the awaitable's additional reference.
            /// @endif
            AsyncSharedState<T>* m_state{nullptr};
            /// @if RU
            /// Не владеющий executor, передаваемый созданному awaiter-у.
            /// @endif
            /// @if EN
            /// Non-owning executor passed to the created awaiter.
            /// @endif
            IExecutor* m_executor{nullptr};
            /// @if RU
            /// Локальная защита от повторного await этого же объекта.
            /// @endif
            /// @if EN
            /// Local guard against awaiting this object more than once.
            /// @endif
            bool m_awaited{false};

            /// @if RU
            /// Удерживает state до уничтожения awaitable.
            /// @param state Состояние, на которое создаётся принадлежащая ссылка.
            /// @param executor Не владеющий executor для возобновления waiter-а.
            /// @endif
            /// @if EN
            /// Retains the state until the awaitable is destroyed.
            /// @param state State for which the owned reference is created.
            /// @param executor Non-owning executor used to resume the waiter.
            /// @endif
            FutureAwaitable(AsyncSharedState<T>* state, IExecutor* executor) noexcept
                : m_state(state), m_executor(executor) {
                if (m_state) {
                    m_state->retain();
                }
            }

            /// @if RU
            /// Освобождает ссылку awaitable.
            /// @endif
            /// @if EN
            /// Releases the awaitable's reference.
            /// @endif
            ~FutureAwaitable() noexcept {
                if (m_state) {
                    m_state->release();
                }
            }

            /// @if RU
            /// Awaitable move-only из-за принадлежащей ссылки на state.
            /// @endif
            /// @if EN
            /// Awaitable is move-only because it owns a reference to the state.
            /// @endif
            FutureAwaitable(const FutureAwaitable&) = delete;
            FutureAwaitable& operator=(const FutureAwaitable&) = delete;

            /// @if RU
            /// Передаёт ссылку из источника.
            /// @param other Источник, после перемещения не владеющий state.
            /// @endif
            /// @if EN
            /// Transfers the reference from the source.
            /// @param other Source, which no longer owns the state after the move.
            /// @endif
            FutureAwaitable(FutureAwaitable&& other) noexcept
                : m_state(std::exchange(other.m_state, nullptr)),
                  m_executor(other.m_executor),
                  m_awaited(other.m_awaited) {}

            /// @if RU
            /// Освобождает прежнюю ссылку и принимает ссылку источника.
            /// @param other Источник; самоприсваивание безопасно.
            /// @return Ссылка на этот awaitable.
            /// @endif
            /// @if EN
            /// Releases the previous reference and takes the source's reference.
            /// @param other Source; self-assignment is safe.
            /// @return Reference to this awaitable.
            /// @endif
            FutureAwaitable& operator=(FutureAwaitable&& other) noexcept {
                if (this != &other) {
                    if (m_state) {
                        m_state->release();
                    }
                    m_state = std::exchange(other.m_state, nullptr);
                    m_executor = other.m_executor;
                    m_awaited = other.m_awaited;
                }
                return *this;
            }

            /// @if RU
            /// Резервирует единственное потребление результата.
            /// @return false при повторном await или отсутствующем state.
            /// @endif
            /// @if EN
            /// Claims the single consumption of the result.
            /// @return false if already awaited or if no state is present.
            /// @endif
            [[nodiscard]] bool claimAwait() noexcept {
                if (m_awaited || !m_state || !m_state->claimConsumer()) {
                    return false;
                }
                m_awaited = true;
                return true;
            }

            /// @if RU
            /// Создаёт awaiter с указанным флагом отмены родительской задачи.
            /// @param cancelFlag Не владеющий указатель либо nullptr.
            /// @return Move-only awaiter, удерживающий собственную state-ссылку.
            /// @endif
            /// @if EN
            /// Creates an awaiter with the specified parent-task cancellation flag.
            /// @param cancelFlag Non-owning pointer or nullptr.
            /// @return Move-only awaiter holding its own state reference.
            /// @endif
            FutureAwaiter prepareAwaiter(std::atomic<bool>* cancelFlag) noexcept {
                return FutureAwaiter{m_state, m_executor, cancelFlag};
            }

            /// @if RU
            /// Await-ит временный awaitable; повторное использование завершает процесс.
            /// @endif
            /// @if EN
            /// Awaits a temporary awaitable; reusing it terminates the process.
            /// @endif
            FutureAwaiter operator co_await() && noexcept {
                if (!claimAwait()) [[unlikely]] {
                    std::terminate();
                }
                return prepareAwaiter(nullptr);
            }
        };

        /// @if RU
        /// Создаёт awaitable, настроенный на executor возобновления.
        /// @param executor Не владеющий executor; должен жить до завершения ожидания.
        /// @endif
        /// @if EN
        /// Creates an awaitable configured with a resumption executor.
        /// @param executor Non-owning executor; must remain alive until the wait completes.
        /// @endif
        [[nodiscard]] FutureAwaitable prepareAwaitable(IExecutor* executor) noexcept {
            return FutureAwaitable{m_state, executor};
        }

        /// @if RU
        /// @return true, если состояние существует и уже сигнализировано.
        /// @endif
        /// @if EN
        /// @return true if the state exists and is already signaled.
        /// @endif
        [[nodiscard]] bool isSignaled() const noexcept {
            return m_state && m_state->isSignaled();
        }

        /// @if RU
        /// @return true, если Future удерживает состояние.
        /// @endif
        /// @if EN
        /// @return true if the Future holds a state.
        /// @endif
        [[nodiscard]] bool hasState() const noexcept {
            return m_state != nullptr;
        }

        /// @if RU
        /// Атомарно резервирует запуск состояния; false для пустого/уже запущенного Future.
        /// @endif
        /// @if EN
        /// Atomically claims the state start; false for an empty/already-started Future.
        /// @endif
        [[nodiscard]] bool beginStart() noexcept {
            return m_state && m_state->beginStart();
        }

        /// @if RU
        /// Сбрасывает отметку старта, если состояние существует.
        /// @endif
        /// @if EN
        /// Resets the started flag if the state exists.
        /// @endif
        void resetStarted() noexcept {
            if (m_state) {
                m_state->resetStarted();
            }
        }

        /// @if RU
        /// Помечает состояние запущенным, если оно существует.
        /// @endif
        /// @if EN
        /// Marks the state as started if it exists.
        /// @endif
        void markStarted() noexcept {
            if (m_state) {
                m_state->markStarted();
            }
        }

        /// @if RU
        /// Устанавливает отмену, если состояние существует.
        /// @endif
        /// @if EN
        /// Sets cancellation if the state exists.
        /// @endif
        void cancel() noexcept {
            if (m_state) {
                m_state->cancel();
            }
        }

        /// @if RU
        /// @return true для пустого Future или отменённого состояния.
        /// @endif
        /// @if EN
        /// @return true for an empty Future or a cancelled state.
        /// @endif
        [[nodiscard]] bool isCanceled() const noexcept {
            return !m_state || m_state->isCanceled();
        }

        /// @if RU
        /// Возвращает не владеющий адрес флага отмены либо nullptr для пустого Future.
        /// @endif
        /// @if EN
        /// Returns a non-owning address of the cancellation flag, or nullptr for an empty Future.
        /// @endif
        [[nodiscard]] std::atomic<bool>* cancellationFlag() noexcept {
            return m_state ? m_state->cancellationFlag() : nullptr;
        }

        /// @if RU
        /// Резервирует единственное потребление результата.
        /// @endif
        /// @if EN
        /// Claims the single consumption of the result.
        /// @endif
        [[nodiscard]] bool claimConsumer() noexcept {
            return m_state && m_state->claimConsumer();
        }

    private:
        friend class AsyncStateFactory<T>;
        /// @if RU
        /// Принимает фабричную ссылку без retain; вызывается только фабрикой.
        /// @endif
        /// @if EN
        /// Takes the factory reference without retaining it; called only by the factory.
        /// @endif
        explicit AsyncFuture(AsyncSharedState<T>* state) noexcept : m_state(state) {}

        /// @if RU
        /// Единственная принадлежащая Future ссылка на state.
        /// @endif
        /// @if EN
        /// The sole state reference owned by this Future.
        /// @endif
        AsyncSharedState<T>* m_state{nullptr};
    };

    /**
     * @if RU
     * @brief Move-only producer-владелец общего состояния.
     *
     * Promise держит одну intrusive-ссылку. Уничтожение незавершённого promise
     * вызывает abandon(): отменяет состояние и сигнализирует зарегистрированного
     * waiter-а. Значение размещается placement new в динамическом allocator-owned
     * блоке; ровно один вызов setValue/storeValue допустим.
     *
     * @tparam T Тип результата или void.
     * @endif
     * @if EN
     * @brief Move-only producer owner of shared state.
     *
     * The promise holds one intrusive reference. Destroying an incomplete promise
     * calls abandon(), cancelling the state and signaling its registered waiter.
     * The value is placement-constructed in a dynamically allocated
     * allocator-owned block; setValue/storeValue may be called exactly once.
     *
     * @tparam T Result type or void.
     * @endif
     */
    template <typename T>
    class AsyncPromise final {
    public:
        /// @if RU
        /// Создаёт пустой promise.
        /// @endif
        /// @if EN
        /// Creates an empty promise.
        /// @endif
        AsyncPromise() noexcept = default;

        /// @if RU
        /// Отменяет и сигнализирует незавершённое состояние, затем освобождает ссылку.
        /// @endif
        /// @if EN
        /// Cancels and signals an incomplete state, then releases the reference.
        /// @endif
        ~AsyncPromise() noexcept {
            if (m_state) {
                m_state->abandon();
                m_state->release();
            }
        }

        /// @if RU
        /// Копирование запрещено: producer-ссылка имеет единственного владельца.
        /// @endif
        /// @if EN
        /// Copying is disabled because the producer reference has a single owner.
        /// @endif
        AsyncPromise(const AsyncPromise&) = delete;
        AsyncPromise& operator=(const AsyncPromise&) = delete;

        /// @if RU
        /// Передаёт владение ссылкой и оставляет источник пустым.
        /// @param other Источник владения.
        /// @endif
        /// @if EN
        /// Transfers ownership of the reference, leaving the source empty.
        /// @param other Ownership source.
        /// @endif
        AsyncPromise(AsyncPromise&& other) noexcept
            : m_state(std::exchange(other.m_state, nullptr)) {}

        /// @if RU
        /// При необходимости отменяет прежнее состояние и принимает ссылку источника.
        /// @param other Источник владения; самоприсваивание безопасно.
        /// @return Ссылка на этот Promise.
        /// @endif
        /// @if EN
        /// Cancels the previous state if needed and takes the source's reference.
        /// @param other Ownership source; self-assignment is safe.
        /// @return Reference to this Promise.
        /// @endif
        AsyncPromise& operator=(AsyncPromise&& other) noexcept {
            if (this != &other) {
                if (m_state) {
                    m_state->abandon();
                    m_state->release();
                }
                m_state = std::exchange(other.m_state, nullptr);
            }
            return *this;
        }

        /**
         * @if RU
         * @brief Конструирует значение перемещением и немедленно завершает состояние.
         * @tparam U Должен совпадать с T.
         * @param value Значение того же типа T.
         * @pre Значение ещё не установлено.
         * @post При duplicate set или пустом promise вызывается std::terminate.
         * @note Функция noexcept; бросающее перемещение завершается аварийно.
         * @endif
         * @if EN
         * @brief Constructs the value by moving and immediately completes the state.
         * @tparam U Must be the same as T.
         * @param value Value of type T.
         * @pre The value has not yet been set.
         * @post A duplicate set or an empty promise calls std::terminate.
         * @note The function is noexcept; a throwing move terminates the process.
         * @endif
         */
        template <typename U = T>
        void setValue(U&& value) noexcept
            requires (!std::same_as<U, void> && std::same_as<U, T>) {
            if (m_state && m_state->claimValue()) [[likely]] {
                ::new (m_state->valueBuffer()) T(std::move(value));
                static_cast<void>(m_state->signalReady());
            } else {
                std::terminate();
            }
        }

        /**
         * @if RU
         * @brief Копирует значение и немедленно завершает состояние.
         * @tparam U Должен совпадать с T.
         * @param value Копируемое значение T.
         * @pre T копируемый, значение ещё не установлено.
         * @endif
         * @if EN
         * @brief Copies the value and immediately completes the state.
         * @tparam U Must be the same as T.
         * @param value Value of T to copy.
         * @pre T is copyable and the value has not yet been set.
         * @endif
         */
        template <typename U = T>
        void setValue(const U& value) noexcept
            requires (!std::same_as<U, void> && std::same_as<U, T>
                && std::copy_constructible<U>) {
            if (m_state && m_state->claimValue()) [[likely]] {
                ::new (m_state->valueBuffer()) T(value);
                static_cast<void>(m_state->signalReady());
            } else {
                std::terminate();
            }
        }

        /**
         * @if RU
         * @brief Завершает состояние без значения для T == void.
         * @pre Это первый вызов завершения для состояния.
         * @post При повторном вызове или пустом promise вызывается std::terminate.
         * @endif
         * @if EN
         * @brief Completes the state without a value when T == void.
         * @pre This is the first completion call for the state.
         * @post A repeated call or an empty promise calls std::terminate.
         * @endif
         */
        void setValue() noexcept requires std::same_as<T, void> {
            if (m_state && m_state->claimValue()) [[likely]] {
                static_cast<void>(m_state->signalReady());
            } else {
                std::terminate();
            }
        }

        /**
         * @if RU
         * @brief Размещает значение, не сигнализируя завершение.
         *
         * Используется coroutine promise: final awaiter публикует завершение
         * после return_value, до уничтожения coroutine frame.
         * @tparam U Должен совпадать с T.
         * @param value Значение для placement construction.
         * @pre Это единственный вызов установки значения.
         * @endif
         * @if EN
         * @brief Stores the value without signaling completion.
         *
         * Used by a coroutine promise: the final awaiter publishes completion
         * after return_value and before the coroutine frame is destroyed.
         * @tparam U Must be the same as T.
         * @param value Value to construct in place.
         * @pre This is the only call that sets the value.
         * @endif
         */
        template <typename U = T>
        void storeValue(U&& value) noexcept
            requires (!std::same_as<U, void> && std::same_as<U, T>) {
            if (m_state && m_state->claimValue()) {
                ::new (m_state->valueBuffer()) T(std::move(value));
            } else {
                std::terminate();
            }
        }

        /// @if RU
        /// Копирует результат в storage без сигнализации (копируемые T).
        /// @tparam U Должен совпадать с T.
        /// @param value Копируемое значение.
        /// @endif
        /// @if EN
        /// Copies the result into storage without signaling (for copyable T).
        /// @tparam U Must be the same as T.
        /// @param value Value to copy.
        /// @endif
        template <typename U = T>
        void storeValue(const U& value) noexcept
            requires (!std::same_as<U, void> && std::same_as<U, T>
                && std::copy_constructible<U>) {
            if (m_state && m_state->claimValue()) {
                ::new (m_state->valueBuffer()) T(value);
            } else {
                std::terminate();
            }
        }

        /// @if RU
        /// Проверяет, что значение для T существует, затем публикует сигнализацию.
        /// Для T == void сигнализирует завершение без payload.
        /// @endif
        /// @if EN
        /// Verifies that a value exists for T, then publishes the signal.
        /// For T == void, signals completion without a payload.
        /// @endif
        void complete() noexcept {
            if (!m_state) [[unlikely]] {
                std::terminate();
            }
            if constexpr (!std::same_as<T, void>) {
                if (!m_state->hasValue()) [[unlikely]] {
                    std::terminate();
                }
            }
            static_cast<void>(m_state->signalReady());
        }

        /// @if RU
        /// Возвращает не владеющий адрес состояния для type-erased final awaiter.
        /// @endif
        /// @if EN
        /// Returns the non-owning state address for the type-erased final awaiter.
        /// @endif
        [[nodiscard]] void* stateAddress() const noexcept {
            return m_state;
        }

        /// @if RU
        /// Type-erased retain для final awaiter; state должен быть валиден.
        /// @endif
        /// @if EN
        /// Type-erased retain operation for the final awaiter; state must be valid.
        /// @endif
        static void retainState(void* state) noexcept {
            static_cast<AsyncSharedState<T>*>(state)->retain();
        }

        /// @if RU
        /// Type-erased release для final awaiter.
        /// @endif
        /// @if EN
        /// Type-erased release operation for the final awaiter.
        /// @endif
        static void releaseState(void* state) noexcept {
            static_cast<AsyncSharedState<T>*>(state)->release();
        }

        /// @if RU
        /// Type-erased публикация завершения; возвращает наличие пробуждённого waiter-а.
        /// @endif
        /// @if EN
        /// Type-erased completion publisher; returns whether a waiter was awakened.
        /// @endif
        static bool completeState(void* state) noexcept {
            return static_cast<AsyncSharedState<T>*>(state)->signalReady();
        }

        /// @if RU
        /// Возвращает не владеющий указатель на флаг отмены либо nullptr.
        /// @endif
        /// @if EN
        /// Returns a non-owning pointer to the cancellation flag, or nullptr.
        /// @endif
        [[nodiscard]] std::atomic<bool>* cancellationFlag() noexcept {
            return m_state ? m_state->cancellationFlag() : nullptr;
        }

    private:
        friend class AsyncStateFactory<T>;
        /// @if RU
        /// Принимает фабричную ссылку без retain; вызывается только фабрикой.
        /// @endif
        /// @if EN
        /// Takes the factory reference without retaining it; called only by the factory.
        /// @endif
        explicit AsyncPromise(AsyncSharedState<T>* state) noexcept : m_state(state) {}

        /// @if RU
        /// Единственная принадлежащая Promise ссылка на state.
        /// @endif
        /// @if EN
        /// The sole state reference owned by this Promise.
        /// @endif
        AsyncSharedState<T>* m_state{nullptr};
    };

    /**
     * @if RU
     * @brief Перемещаемая пара владельцев одного AsyncSharedState.
     *
     * Promise и Future получают по одной начальной ссылке на общее состояние.
     * Пара и оба её элемента move-only.
     * @tparam T Тип результата или void.
     * @endif
     * @if EN
     * @brief Movable pair of owners of one AsyncSharedState.
     *
     * The promise and future each receive one initial reference to the shared
     * state. The pair and both of its members are move-only.
     * @tparam T Result type or void.
     * @endif
     */
    template <typename T>
    struct AsyncStatePair {
        /// @if RU
        /// Producer-владелец результата.
        /// @endif
        /// @if EN
        /// Producer owner of the result.
        /// @endif
        AsyncPromise<T> promise;
        /// @if RU
        /// Consumer-владелец результата.
        /// @endif
        /// @if EN
        /// Consumer owner of the result.
        /// @endif
        AsyncFuture<T> future;

        /// @if RU
        /// Принимает оба владельца по перемещению.
        /// @endif
        /// @if EN
        /// Takes ownership of both owners by move.
        /// @endif
        AsyncStatePair(AsyncPromise<T>&& statePromise, AsyncFuture<T>&& stateFuture) noexcept
            : promise(std::move(statePromise)), future(std::move(stateFuture)) {}

        /// @if RU
        /// Копирование пары запрещено.
        /// @endif
        /// @if EN
        /// Copying the pair is disabled.
        /// @endif
        AsyncStatePair(const AsyncStatePair&) = delete;
        AsyncStatePair& operator=(const AsyncStatePair&) = delete;
        /// @if RU
        /// Передаёт оба владения.
        /// @endif
        /// @if EN
        /// Transfers ownership of both members.
        /// @endif
        AsyncStatePair(AsyncStatePair&&) noexcept = default;
        /// @if RU
        /// Перемещает оба владельца с освобождением прежнего содержимого.
        /// @endif
        /// @if EN
        /// Moves both owners, releasing their previous contents.
        /// @endif
        AsyncStatePair& operator=(AsyncStatePair&&) noexcept = default;
    };

    /**
     * @if RU
     * @brief Фабрика allocator-owned AsyncSharedState.
     *
     * Выделяет один AllocationBlock, содержащий allocator и state, чтобы state
     * сохранил возможность деаллоцировать себя тем же rebound allocator после
     * уничтожения внешней пары. Требуется allocator с noexcept rebind-копированием,
     * noexcept destruction/deallocate; исключение из allocate переводится в
     * Error::allocation_failure.
     * @tparam T Тип результата или void.
     * @endif
     * @if EN
     * @brief Factory for allocator-owned AsyncSharedState.
     *
     * Allocates one AllocationBlock containing both the allocator and state, so
     * the state can deallocate itself using the same rebound allocator after the
     * external pair is destroyed. The allocator must support noexcept rebind
     * copying and noexcept destruction/deallocation; exceptions from allocate
     * are converted to Error::allocation_failure.
     * @tparam T Result type or void.
     * @endif
     */
    template <typename T>
    class AsyncStateFactory final {
        using State = AsyncSharedState<T>;

    public:
        /**
         * @if RU
         * @brief Создаёт состояние и пару владельцев с заданным allocator-ом.
         * @tparam Allocator Allocator, удовлетворяющий требуемым noexcept условиям.
         * @param allocator Экземпляр allocator-а, копируемый/rebind в блоке состояния.
         * @param started Начальное значение флага запуска.
         * @return Пара promise/future либо allocation_failure.
         * @note Функция noexcept; ошибки allocate (включая выброшенные исключения)
         *       переводятся в Result. Инициализация T откладывается до setValue.
         * @endif
         * @if EN
         * @brief Creates state and a pair of owners using the specified allocator.
         * @tparam Allocator Allocator satisfying the required noexcept conditions.
         * @param allocator Allocator instance copied/rebound into the state block.
         * @param started Initial value of the started flag.
         * @return Promise/future pair or allocation_failure.
         * @note The function is noexcept; allocation errors, including thrown
         *       exceptions, are converted to Result. T is initialized later by setValue.
         * @endif
         */
        template <typename Allocator = MiAllocator<std::byte>>
        [[nodiscard]] static Result<AsyncStatePair<T>> create(
            const Allocator& allocator = Allocator{},
            bool started = false) noexcept
        {
            using State = AsyncSharedState<T>;
            using Allocation = AllocationBlock<Allocator>;
            using ReboundAllocator =
                typename std::allocator_traits<Allocator>::template rebind_alloc<Allocation>;
            using Traits = std::allocator_traits<ReboundAllocator>;

            static_assert(std::is_nothrow_constructible_v<
                    ReboundAllocator, const Allocator&>,
                "Async state allocators must be nothrow rebind constructible");
            static_assert(noexcept(
                    std::declval<ReboundAllocator&>().deallocate(
                        std::declval<Allocation*>(), size_t{1})),
                "Async state allocators must provide noexcept deallocate");
            static_assert(std::is_nothrow_copy_constructible_v<ReboundAllocator>,
                "Async state allocators must be nothrow copy constructible");
            static_assert(std::is_nothrow_destructible_v<ReboundAllocator>,
                "Async state allocators must be nothrow destructible");

            ReboundAllocator rebound{allocator};
            Allocation* allocation = nullptr;
            try {
                allocation = Traits::allocate(rebound, 1);
            } catch (...) {
                return std::unexpected(Error::allocation_failure);
            }
            if (allocation == nullptr) [[unlikely]] {
                return std::unexpected(Error::allocation_failure);
            }

            auto* const block = ::new (static_cast<void*>(allocation))
                Allocation{rebound, started};
            State* const state = &block->state;
            block->state.setAllocation(block, [](State* stateToDestroy) noexcept {
                auto* const owner =
                    static_cast<Allocation*>(stateToDestroy->allocation());
                ReboundAllocator ownerAllocator{owner->allocator};
                Traits::destroy(ownerAllocator, owner);
                Traits::deallocate(ownerAllocator, owner, 1);
            });
            return AsyncStatePair<T>{
                AsyncPromise<T>{state},
                AsyncFuture<T>{state}
            };
        }

    private:
        template <typename Allocator>
        struct AllocationBlock {
            using ReboundAllocator =
                typename std::allocator_traits<Allocator>::template rebind_alloc<AllocationBlock>;

            /// @if RU
            /// Сохраняет allocator рядом с состоянием до последнего release.
            /// @endif
            /// @if EN
            /// Keeps the allocator beside the state until the final release.
            /// @endif
            AllocationBlock(const ReboundAllocator& stateAllocator, bool started) noexcept
                : allocator(stateAllocator),
                  state(2, started, this, nullptr) {}

            /// @if RU
            /// Экземпляр, который должен деаллоцировать этот же блок.
            /// @endif
            /// @if EN
            /// Allocator instance that must deallocate this same block.
            /// @endif
            ReboundAllocator allocator;
            /// @if RU
            /// Inline общее состояние с callback-ом удаления блока.
            /// @endif
            /// @if EN
            /// Inline shared state with a callback that deletes the block.
            /// @endif
            State state;
        };
    };

    /**
     * @if RU
     * @brief Удобная фабрика общей пары promise/future.
     * @tparam T Тип результата или void.
     * @tparam Allocator Allocator для единого динамического блока состояния.
     * @param allocator Аллокатор; по умолчанию MiAllocator<std::byte>.
     * @param started Начальное значение флага запуска.
     * @return Пара move-only владельцев либо Error::allocation_failure.
     * @endif
     * @if EN
     * @brief Convenience factory for a shared promise/future pair.
     * @tparam T Result type or void.
     * @tparam Allocator Allocator for the single dynamically allocated state block.
     * @param allocator Allocator; defaults to MiAllocator<std::byte>.
     * @param started Initial value of the started flag.
     * @return Pair of move-only owners or Error::allocation_failure.
     * @endif
     */
    template <typename T, typename Allocator = MiAllocator<std::byte>>
    [[nodiscard]] Result<AsyncStatePair<T>> makeAsyncState(
        const Allocator& allocator = Allocator{},
        bool started = false) noexcept
    {
        return AsyncStateFactory<T>::create(allocator, started);
    }

}
