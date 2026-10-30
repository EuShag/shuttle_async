#pragma once

#include <atomic>
#include <concepts>
#include <coroutine>
#include <exception>
#include <memory>
#include <type_traits>
#include <utility>

#include <async/async_result.hpp>
#include <async/error.hpp>
#include <async/i_executor.hpp>
#include <async/mi_allocator.hpp>
#include <async/async_event.hpp>

namespace shuttle::async {

    /**
     * @brief Lifecycle phases of a task's shared state.
     *
     * Transitions are atomic: Pending may transition to Suspended or Signaled,
     * and Suspended may transition to Signaled. Repeated completion does not
     * signal a waiter more than once.
     */
    enum class AsyncTaskState : uint32_t {
        /// State has been created; completion has not yet been published.
        Pending = 0,
        /// The single waiting coroutine handle has been registered.
        Suspended = 1,
        /// Completion or cancellation has been published.
        Signaled = 2
    };

    template <typename T> class AsyncSharedState;
    template <typename T> class AsyncFuture;
    template <typename T> class AsyncPromise;
    template <typename T> class AsyncStateFactory;
    template <typename T> struct AsyncStatePair;

    /**
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
     */
    template <typename T>
    class AsyncSharedState final {
    public:
        /// Destroys the constructed value, if any; the final release() frees the block.
        ~AsyncSharedState() noexcept {
            if constexpr (!std::same_as<T, void>) {
                if (isSignaled() && hasValue()) {
                    reinterpret_cast<T*>(m_valueBuf)->~T();
                }
            }
        }

        /// State is non-copyable; its lifetime is managed by the reference count.
        AsyncSharedState(const AsyncSharedState&) = delete;
        AsyncSharedState& operator=(const AsyncSharedState&) = delete;

        /// Adds an owning reference; the caller must later call release().
        void retain() noexcept {
            m_referenceCount.fetch_add(1, std::memory_order_relaxed);
        }

        /// Removes an owning reference and destroys the allocator block on the final reference.
        void release() noexcept {
            if (m_referenceCount.fetch_sub(1, std::memory_order_acq_rel) == 1) [[unlikely]] {
                m_destroyAllocation(this);
            }
        }

        bool prepare_wait(WaitNode* node) {
            return m_futex.prepareWait(node);
        }

        /// @return true if completion or cancellation has been signaled.
        [[nodiscard]] bool isSignaled() const noexcept {
            // Теперь готовность — это просто единица в m_futexState,
            // которую выставляет метод signalReady()
            return m_hasResult.load(std::memory_order_acquire) == 1;
        }

        /// Atomically claims the single consumption of the result.
        /// @return true only for the first call.
        [[nodiscard]] bool claimConsumer() noexcept {
            bool expected = false;
            return m_consumerClaimed.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel, std::memory_order_relaxed);
        }

        /// Atomically claims the single write of a value.
        /// @return true only for the first call.
        [[nodiscard]] bool claimValue() noexcept {
            bool expected = false;
            return m_valueSet.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel, std::memory_order_relaxed);
        }

        /// @return true if the producer has placed a value in inline storage.
        [[nodiscard]] bool hasValue() const noexcept {
            return m_valueSet.load(std::memory_order_acquire);
        }

        /// Atomically marks the task as started.
        /// @return true if this call was the first to claim the start.
        [[nodiscard]] bool beginStart() noexcept {
            bool expected = false;
            return m_started.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel, std::memory_order_acquire);
        }

        /// Clears the started flag, for example after an executor rejects the task.
        void resetStarted() noexcept {
            m_started.store(false, std::memory_order_release);
        }

        /// Marks the task as started without checking its previous state.
        void markStarted() noexcept {
            m_started.store(true, std::memory_order_release);
        }

        void cancel() noexcept {
            // 1. Взводим флаг отмены с семантикой Release, чтобы все потоки увидели это
            m_waitNode.parentCancelledFlag.store(true, std::memory_order_release);
            AsyncEvent::tryCancel(&m_waitNode);
        }

        /// Marks an incomplete state as cancelled and publishes completion.
        /// An already completed state is unchanged.
        void abandon() noexcept {
            if (!isSignaled()) {
                static_cast<void>(signalReady());
            }
        }

        /// @return true if the cancellation flag is set.
        [[nodiscard]] bool isCanceled() const noexcept {
            return m_waitNode.parentCancelledFlag.load(std::memory_order_acquire);
        }

        /// Returns the address of the atomic cancellation flag for awaiters.
        /// The pointer is non-owning and is valid only while the state is alive.
        [[nodiscard]] WaitNode* waitNode() noexcept {
            return &m_waitNode;
        }

        /**
         * @brief Publishes completion and resumes the waiter, if needed.
         * @return true if a waiter was registered; resumption may have been
         *         submitted to the executor or performed synchronously after an error.
         * @note If execute() fails, the waiter resumes directly on the calling
         *       thread. A repeated signal does not resume it again.
         */
        // ЗАМЕНИТЬ СТАРЫЙ signalReady НА ЭТО:
        [[nodiscard]] bool signalReady() noexcept {
            // 1. Атомарно объявляем, что данные готовы (переводим из 0 в 1)
            m_hasResult.store(true, std::memory_order_release);

            // 2. Будим абсолютно всех, кто успел сесть на наш футекс.
            // Интрузивный футекс сам знает, на каких экзекьюторах их нужно продолжить.
            m_futex.notifyAll();

            return true;
        }


        /// Returns a reference to the result, moving it from storage.
        /// Call only after successful completion and the sole claimConsumer().
        template <typename U = T>
        [[nodiscard]] U&& extractValue() noexcept requires (!std::same_as<U, void>) {
            return std::move(*reinterpret_cast<U*>(m_valueBuf));
        }

        /// Gets storage for placement-new construction of the result.
        /// @return Address of the inline buffer; the caller is responsible for constructing exactly once.
        template <typename U = T>
        [[nodiscard]] void* valueBuffer() noexcept requires (!std::same_as<U, void>) {
            return static_cast<void*>(m_valueBuf);
        }

    private:
        template <typename> friend class AsyncStateFactory;

        using DestroyAllocation = void(*)(AsyncSharedState*) noexcept;
        using ValueStorage = std::conditional_t<std::same_as<T, void>, std::max_align_t, T>;

        /**
         * @brief Initializes internal state; accessible only to the factory.
         * @param references Initial reference count for the owners (Promise and Future).
         * @param started Initial value of the started flag.
         * @param allocation Beginning of the allocator-owned block.
         * @param destroyAllocation Callback that frees the entire block.
         */
        AsyncSharedState(
            uint32_t references,
            bool started,
            void* allocation,
            DestroyAllocation destroyAllocation) noexcept
            : m_referenceCount(references),
              m_started(started),
              m_allocation(allocation),
              m_destroyAllocation(destroyAllocation) {}

        /**
         * @brief Sets the allocator-block deallocation callback data.
         * @param allocation Starting address of the block containing the state.
         * @param destroyAllocation Block destructor, called by the final reference.
         */
        void setAllocation(void* allocation, DestroyAllocation destroyAllocation) noexcept {
            m_allocation = allocation;
            m_destroyAllocation = destroyAllocation;
        }

        /// Returns the beginning of the allocator-owned block used by the final release.
        [[nodiscard]] void* allocation() const noexcept {
            return m_allocation;
        }

        /// Number of owning references; initially two (Promise and Future).
        std::atomic<uint32_t> m_referenceCount;
        /// Guards the task's one-time start.
        std::atomic<bool> m_started;
        /// Cooperative cancellation flag; setting it is separate from signaling.
        WaitNode m_waitNode{};
        /// Claimed before placement-constructing the result.
        std::atomic<bool> m_valueSet{false};
        /// Atomic claim for the sole consumer.
        alignas(64) std::atomic<bool> m_consumerClaimed{false};
        // ДОБАВИТЬ:
        // Поле m_state оставляем, но теперь это ПРОСТОЙ атомик флага готовности (0 - не готово, 1 - готово)
        alignas(64) std::atomic<bool> m_hasResult{ false };
        AsyncEvent                        m_futex;

        /// Inline result storage; T exists only after claimValue/placement new.
        alignas(64) char m_valueBuf[std::same_as<T, void> ? 1 : sizeof(ValueStorage)]{};
        /// Start of the AllocationBlock containing the saved rebound allocator.
        void* m_allocation;
        /// Callback that destroys the AllocationBlock and frees it with the same allocator.
        DestroyAllocation m_destroyAllocation;
    };

    /**
     * @brief Move-only consumer owner of shared state.
     *
     * The future owns one intrusive reference. Prepared awaitables and awaiters
     * hold their own references and can therefore outlive a moved-from or
     * destroyed future. Only one awaiter may consume a result. An empty object
     * can be moved or destroyed safely; awaiting it is treated as a cancelled result.
     *
     * @tparam T Result type or void.
     */
    template <typename T>
    class AsyncFuture final {
    public:
        /// Creates an empty Future with no state.
        AsyncFuture() noexcept = default;

        /// Releases the Future's reference, if present.
        ~AsyncFuture() noexcept {
            if (m_state) {
                m_state->release();
            }
        }

        /// Copying is disabled so the consumer reference has a single owner.
        AsyncFuture(const AsyncFuture&) = delete;
        AsyncFuture& operator=(const AsyncFuture&) = delete;

        /// Transfers ownership of the reference, leaving the source empty.
        /// @param other Source, which has no state after the move.
        AsyncFuture(AsyncFuture&& other) noexcept
            : m_state(std::exchange(other.m_state, nullptr)) {}

        /// Releases the previous reference and takes the source's reference.
        /// @param other Ownership source; self-assignment is safe.
        /// @return Reference to this Future.
        AsyncFuture& operator=(AsyncFuture&& other) noexcept {
            if (this != &other) {
                if (m_state) {
                    m_state->release();
                }
                m_state = std::exchange(other.m_state, nullptr);
            }
            return *this;
        }

        AsyncSharedState<T>& getState() {
            return *m_state;
        }

        /**
         * @brief Awaiter that registers a coroutine as a state waiter.
         *
         * Holds its own reference to the state. The executor is a non-owning
         * pointer and must remain alive throughout the wait.
         */
        struct FutureAwaiter final {
            /// Non-owning executor used to resume the waiting coroutine.
            IExecutor* m_executor{nullptr};
            WaitNode node{};

            AsyncFuture m_future;

            /// Retains the state and stores the executor.
            FutureAwaiter(
                AsyncFuture&& future,
                IExecutor* executor) noexcept
                : m_executor(executor), m_future{std::move(future)} {}

            /// Awaiter is non-copyable to avoid ambiguous reference ownership.
            FutureAwaiter(const FutureAwaiter&) = delete;
            FutureAwaiter& operator=(const FutureAwaiter&) = delete;

            /// Moves the reference and wait parameters.
            FutureAwaiter(FutureAwaiter&& other) noexcept :
                m_executor{std::exchange(other.m_executor, nullptr)}, m_future{std::move(other.m_future)} {}
            /// Releases the previous reference and takes the source awaiter's reference.
            FutureAwaiter& operator=(FutureAwaiter&& other) noexcept {
                if (this == &other) return *this;
                m_executor = std::exchange(other.m_executor, nullptr);
                m_future = std::move(other.m_future);
                return *this;
            }

            /// @brief Соответствие концепту: проверка готовности с учетом инжектированного флага.
            /// @return true для пустой Future, отмененной задачи или уже сигнального состояния.
            [[nodiscard]] bool await_ready(WaitNode* pCoroNode) const noexcept {
                // Проверяем флаг отмены, который промис уже зашил в ноду
                if (pCoroNode && pCoroNode->parentCancelledFlag &&
                    pCoroNode->parentCancelledFlag.load(std::memory_order_acquire)) [[unlikely]]
                {
                    return true;
                }
                return m_future.isSignaled();
            }

            /**
             * @brief Соответствие концепту: регистрация воркера в футексе с поддержкой флага отмены.
             * @param handle Coroutine that is waiting.
             * @param pCancelled Указатель на атомарный флаг отмены родительской задачи.
             * @return Dispatcher handle if the wait was registered; otherwise,
             *         the original handle for immediate continuation on the current thread.
             */
            bool await_suspend(std::coroutine_handle<> handle, WaitNode* pCoroNode) noexcept {
                if (!pCoroNode) [[unlikely]] return false;
                if (!m_future.hasState()) [[unlikely]] return false;

                // 1. FAST-PATH ЗАЩИТА: Если отмена взведена ДО вызова — никуда не ложимся,
                // мгновенно возвращаем false для продолжения на текущем ядре!
                // Обратите внимание на исправленный синтаксис стрелочки -> к указателю атомика
                if (pCoroNode->parentCancelledFlag && pCoroNode->parentCancelledFlag.load(std::memory_order_acquire)) [[unlikely]] {
                    return false;
                }

                // Зашиваем хэндл и наш вычислительный экзекьютор прямо в готовую ноду из промиса
                pCoroNode->handle = handle;
                pCoroNode->executor = m_executor;

                // 2. Ложимся в ваш lock-free футекс.
                if (m_future.getState().prepare_wait(pCoroNode)) {

                    // 3. МГНОВЕННАЯ LOCK-FREE ОТМЕНА (Double-Check Barrier):
                    // Если отмена прилетела ровно в ту наносекунду, пока мы пушились в ConcurrentQueue,
                    // мы принудительно выбиваем себя сами из любого потока через tryCancel!
                    if (pCoroNode->parentCancelledFlag && pCoroNode->parentCancelledFlag.load(std::memory_order_acquire)) [[unlikely]] {
                        AsyncEvent::tryCancel(pCoroNode);
                    }

                    return true; // Корутина успешно зарегистрирована, поток уходит к диспетчеру
                }

                return false; // Сигнал уже прилетел на взлете, спать не нужно
            }

            /**
             * @brief Соответствие концепту: извлечение результатов с учетом флага отмены.
             * @param pWaitNode Указатель на атомарный флаг отмены родительской задачи.
             * @return For T, the moved result or a value-initialized T{} on
             *         cancellation; for void, only the cancellation flag.
             */
            [[nodiscard]] auto await_resume(WaitNode* pWaitNode) noexcept {
                const bool cancelled =
                    (pWaitNode->parentCancelledFlag && pWaitNode->parentCancelledFlag.load(std::memory_order_acquire))
                    || m_future.isCanceled();

                if constexpr (std::same_as<T, void>) {
                    return AsyncResult<void>{.isCancelled = cancelled};
                } else {
                    if (cancelled) [[unlikely]] {
                        return AsyncResult<T>{.value = T{}, .isCancelled = true};
                    }
                    return AsyncResult<T>{
                        .value = m_future.getState().extractValue(), .isCancelled = false};
                }
            }
        };

        /// @return true if the state exists and is already signaled.
        [[nodiscard]] bool isSignaled() const noexcept {
            return m_state && m_state->isSignaled();
        }

        /// @return true if the Future holds a state.
        [[nodiscard]] bool hasState() const noexcept {
            return m_state != nullptr;
        }

        /// Atomically claims the state start; false for an empty/already-started Future.
        [[nodiscard]] bool beginStart() noexcept {
            return m_state && m_state->beginStart();
        }

        /// Resets the started flag if the state exists.
        void resetStarted() noexcept {
            if (m_state) {
                m_state->resetStarted();
            }
        }

        /// Marks the state as started if it exists.
        void markStarted() noexcept {
            if (m_state) {
                m_state->markStarted();
            }
        }

        /// Sets cancellation if the state exists.
        void cancel() noexcept {
            if (m_state) {
                m_state->cancel();
            }
        }

        /// @return true for an empty Future or a cancelled state.
        [[nodiscard]] bool isCanceled() const noexcept {
            return !m_state || m_state->isCanceled();
        }

        /// Returns a non-owning address of the cancellation flag, or nullptr for an empty Future.
        [[nodiscard]] std::atomic<bool>* cancellationFlag() noexcept {
            return m_state ? m_state->cancellationFlag() : nullptr;
        }

        /// Claims the single consumption of the result.
        [[nodiscard]] bool claimConsumer() noexcept {
            return m_state && m_state->claimConsumer();
        }

        FutureAwaiter getAwaiter(IExecutor* executor) {
            return {std::move(*this), executor};
        }

    private:
        friend class AsyncStateFactory<T>;
        /// Takes the factory reference without retaining it; called only by the factory.
        explicit AsyncFuture(AsyncSharedState<T>* state) noexcept : m_state(state) {}

        /// The sole state reference owned by this Future.
        AsyncSharedState<T>* m_state{nullptr};
    };

    /**
     * @brief Move-only producer owner of shared state.
     *
     * The promise holds one intrusive reference. Destroying an incomplete promise
     * calls abandon(), cancelling the state and signaling its registered waiter.
     * The value is placement-constructed in a dynamically allocated
     * allocator-owned block; setValue/storeValue may be called exactly once.
     *
     * @tparam T Result type or void.
     */
    template <typename T>
    class AsyncPromise final {
    public:
        /// Creates an empty promise.
        AsyncPromise() noexcept = default;

        /// Cancels and signals an incomplete state, then releases the reference.
        ~AsyncPromise() noexcept {
            if (m_state) {
                m_state->abandon();
                m_state->release();
            }
        }

        /// Copying is disabled because the producer reference has a single owner.
        AsyncPromise(const AsyncPromise&) = delete;
        AsyncPromise& operator=(const AsyncPromise&) = delete;

        /// Transfers ownership of the reference, leaving the source empty.
        /// @param other Ownership source.
        AsyncPromise(AsyncPromise&& other) noexcept
            : m_state(std::exchange(other.m_state, nullptr)) {}

        /// Cancels the previous state if needed and takes the source's reference.
        /// @param other Ownership source; self-assignment is safe.
        /// @return Reference to this Promise.
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
         * @brief Constructs the value by moving and immediately completes the state.
         * @tparam U Must be the same as T.
         * @param value Value of type T.
         * @pre The value has not yet been set.
         * @post A duplicate set or an empty promise calls std::terminate.
         * @note The function is noexcept; a throwing move terminates the process.
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
         * @brief Copies the value and immediately completes the state.
         * @tparam U Must be the same as T.
         * @param value Value of T to copy.
         * @pre T is copyable and the value has not yet been set.
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
         * @brief Completes the state without a value when T == void.
         * @pre This is the first completion call for the state.
         * @post A repeated call or an empty promise calls std::terminate.
         */
        void setValue() noexcept requires std::same_as<T, void> {
            if (m_state && m_state->claimValue()) [[likely]] {
                static_cast<void>(m_state->signalReady());
            } else {
                std::terminate();
            }
        }

        /**
         * @brief Stores the value without signaling completion.
         *
         * Used by a coroutine promise: the final awaiter publishes completion
         * after return_value and before the coroutine frame is destroyed.
         * @tparam U Must be the same as T.
         * @param value Value to construct in place.
         * @pre This is the only call that sets the value.
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

        /// Copies the result into storage without signaling (for copyable T).
        /// @tparam U Must be the same as T.
        /// @param value Value to copy.
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

        /// Verifies that a value exists for T, then publishes the signal.
        /// For T == void, signals completion without a payload.
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

        /// Returns the non-owning state address for the type-erased final awaiter.
        [[nodiscard]] void* stateAddress() const noexcept {
            return m_state;
        }

        /// Type-erased retain operation for the final awaiter; state must be valid.
        static void retainState(void* state) noexcept {
            static_cast<AsyncSharedState<T>*>(state)->retain();
        }

        /// Type-erased release operation for the final awaiter.
        static void releaseState(void* state) noexcept {
            static_cast<AsyncSharedState<T>*>(state)->release();
        }

        /// Type-erased completion publisher; returns whether a waiter was awakened.
        static bool completeState(void* state) noexcept {
            return static_cast<AsyncSharedState<T>*>(state)->signalReady();
        }

        /// Returns a non-owning pointer to the cancellation flag, or nullptr.
        [[nodiscard]] WaitNode* waitNode() noexcept {
            return m_state ? m_state->waitNode() : nullptr;
        }

    private:
        friend class AsyncStateFactory<T>;
        /// Takes the factory reference without retaining it; called only by the factory.
        explicit AsyncPromise(AsyncSharedState<T>* state) noexcept : m_state(state) {}

        /// The sole state reference owned by this Promise.
        AsyncSharedState<T>* m_state{nullptr};
    };

    /**
     * @brief Movable pair of owners of one AsyncSharedState.
     *
     * The promise and future each receive one initial reference to the shared
     * state. The pair and both of its members are move-only.
     * @tparam T Result type or void.
     */
    template <typename T>
    struct AsyncStatePair {
        /// Producer owner of the result.
        AsyncPromise<T> promise;
        /// Consumer owner of the result.
        AsyncFuture<T> future;

        /// Takes ownership of both owners by move.
        AsyncStatePair(AsyncPromise<T>&& statePromise, AsyncFuture<T>&& stateFuture) noexcept
            : promise(std::move(statePromise)), future(std::move(stateFuture)) {}

        /// Copying the pair is disabled.
        AsyncStatePair(const AsyncStatePair&) = delete;
        AsyncStatePair& operator=(const AsyncStatePair&) = delete;
        /// Transfers ownership of both members.
        AsyncStatePair(AsyncStatePair&&) noexcept = default;
        /// Moves both owners, releasing their previous contents.
        AsyncStatePair& operator=(AsyncStatePair&&) noexcept = default;
    };

    /**
     * @brief Factory for allocator-owned AsyncSharedState.
     *
     * Allocates one AllocationBlock containing both the allocator and state, so
     * the state can deallocate itself using the same rebound allocator after the
     * external pair is destroyed. The allocator must support noexcept rebind
     * copying and noexcept destruction/deallocation; exceptions from allocate
     * are converted to Error::allocation_failure.
     * @tparam T Result type or void.
     */
    template <typename T>
    class AsyncStateFactory final {
        using State = AsyncSharedState<T>;

    public:
        /**
         * @brief Creates state and a pair of owners using the specified allocator.
         * @tparam Allocator Allocator satisfying the required noexcept conditions.
         * @param allocator Allocator instance copied/rebound into the state block.
         * @param started Initial value of the started flag.
         * @return Promise/future pair or allocation_failure.
         * @note The function is noexcept; allocation errors, including thrown
         *       exceptions, are converted to Result. T is initialized later by setValue.
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

            /// Keeps the allocator beside the state until the final release.
            AllocationBlock(const ReboundAllocator& stateAllocator, bool started) noexcept
                : allocator(stateAllocator),
                  state(2, started, this, nullptr) {}

            /// Allocator instance that must deallocate this same block.
            ReboundAllocator allocator;
            /// Inline shared state with a callback that deletes the block.
            State state;
        };
    };

    /**
     * @brief Convenience factory for a shared promise/future pair.
     * @tparam T Result type or void.
     * @tparam Allocator Allocator for the single dynamically allocated state block.
     * @param allocator Allocator; defaults to MiAllocator<std::byte>.
     * @param started Initial value of the started flag.
     * @return Pair of move-only owners or Error::allocation_failure.
     */
    template <typename T, typename Allocator = MiAllocator<std::byte>>
    [[nodiscard]] Result<AsyncStatePair<T>> makeAsyncState(
        const Allocator& allocator = Allocator{},
        bool started = false) noexcept
    {
        return AsyncStateFactory<T>::create(allocator, started);
    }

}
