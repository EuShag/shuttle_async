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

        /// @return true if completion or cancellation has been signaled.
        [[nodiscard]] bool isSignaled() const noexcept {
            return m_state.load(std::memory_order_acquire) == AsyncTaskState::Signaled;
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

        /// Sets the cancellation flag; this alone does not publish completion.
        void cancel() noexcept {
            m_isCanceled.store(true, std::memory_order_release);
        }

        /// Marks an incomplete state as cancelled and publishes completion.
        /// An already completed state is unchanged.
        void abandon() noexcept {
            if (!isSignaled()) {
                m_isCanceled.store(true, std::memory_order_release);
                static_cast<void>(signalReady());
            }
        }

        /// @return true if the cancellation flag is set.
        [[nodiscard]] bool isCanceled() const noexcept {
            return m_isCanceled.load(std::memory_order_acquire);
        }

        /// Returns the address of the atomic cancellation flag for awaiters.
        /// The pointer is non-owning and is valid only while the state is alive.
        [[nodiscard]] std::atomic<bool>* cancellationFlag() noexcept {
            return &m_isCanceled;
        }

        /**
         * @brief Registers the sole waiter if the state is still Pending.
         * @param handle Coroutine handle to pass to the executor when signaled.
         * @param executor Non-owning executor used to resume the waiter.
         * @return true if the state became Suspended; otherwise, no waiter was registered.
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
         * @brief Publishes completion and resumes the waiter, if needed.
         * @return true if a waiter was registered; resumption may have been
         *         submitted to the executor or performed synchronously after an error.
         * @note If execute() fails, the waiter resumes directly on the calling
         *       thread. A repeated signal does not resume it again.
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
              m_state(AsyncTaskState::Pending),
              m_handle(nullptr),
              m_executor(nullptr),
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
        std::atomic<bool> m_isCanceled{false};
        /// Claimed before placement-constructing the result.
        std::atomic<bool> m_valueSet{false};
        /// Atomic claim for the sole consumer.
        alignas(64) std::atomic<bool> m_consumerClaimed{false};
        /// State machine Pending -> Suspended/Signaled.
        alignas(64) std::atomic<AsyncTaskState> m_state;
        /// Sole registered waiter; the handle is non-owning.
        alignas(64) std::coroutine_handle<> m_handle;
        /// Non-owning executor for the waiter; it must outlive the wait.
        IExecutor* m_executor;
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

        /**
         * @brief Awaiter that registers a coroutine as a state waiter.
         *
         * Holds its own reference to the state. The executor and cancellation
         * flag are non-owning pointers and must remain alive throughout the wait.
         */
        struct FutureAwaiter {
            /// State held by this awaiter's additional reference.
            AsyncSharedState<T>* m_state{nullptr};
            /// Non-owning executor used to resume the waiting coroutine.
            IExecutor* m_executor{nullptr};
            /// @if RU
            /// Не владеющий флаг отмены родительской задачи.
            /// @endif
            /// @if EN
            /// Non-owning cancellation flag of the parent task.
            /// @endif
            std::atomic<bool>* parentCancelledFlag{nullptr};

            /// Retains the state and stores the executor/cancellation flag.
            FutureAwaiter(
                AsyncSharedState<T>* state,
                IExecutor* executor,
                std::atomic<bool>* cancellationFlag) noexcept
                : m_state(state), m_executor(executor), parentCancelledFlag(cancellationFlag) {
                if (m_state) {
                    m_state->retain();
                }
            }

            /// Releases the state reference owned by the awaiter.
            ~FutureAwaiter() noexcept {
                if (m_state) {
                    m_state->release();
                }
            }

            /// Awaiter is non-copyable to avoid ambiguous reference ownership.
            FutureAwaiter(const FutureAwaiter&) = delete;
            FutureAwaiter& operator=(const FutureAwaiter&) = delete;

            /// Moves the reference and wait parameters.
            FutureAwaiter(FutureAwaiter&& other) noexcept
                : m_state(std::exchange(other.m_state, nullptr)),
                  m_executor(other.m_executor),
                  parentCancelledFlag(other.parentCancelledFlag) {}

            /// Releases the previous reference and takes the source awaiter's reference.
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

            /// @return true for an empty Future or an already signaled state.
            [[nodiscard]] bool await_ready() const noexcept {
                return !m_state || m_state->isSignaled();
            }

            /**
             * @brief Registers the waiter or continues immediately.
             * @param handle Coroutine that is waiting.
             * @return Dispatcher handle if the wait was registered; otherwise,
             *         the original handle for immediate continuation.
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
             * @brief Returns AsyncResult after wakeup.
             * @return For T, the moved result or a value-initialized T{} on
             *         cancellation; for void, only the cancellation flag.
             * @note A cancelled T requires T to be default-initializable.
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
         * @brief Awaitable object that provides one-time access to a FutureAwaiter.
         *
         * Holds its own reference to the state until destruction; only one
         * awaiter may be obtained from the awaitable/Future for this state.
         */
        struct FutureAwaitable {
            /// State held by the awaitable's additional reference.
            AsyncSharedState<T>* m_state{nullptr};
            /// Non-owning executor passed to the created awaiter.
            IExecutor* m_executor{nullptr};
            /// Local guard against awaiting this object more than once.
            bool m_awaited{false};

            /// Retains the state until the awaitable is destroyed.
            /// @param state State for which the owned reference is created.
            /// @param executor Non-owning executor used to resume the waiter.
            FutureAwaitable(AsyncSharedState<T>* state, IExecutor* executor) noexcept
                : m_state(state), m_executor(executor) {
                if (m_state) {
                    m_state->retain();
                }
            }

            /// Releases the awaitable's reference.
            ~FutureAwaitable() noexcept {
                if (m_state) {
                    m_state->release();
                }
            }

            /// Awaitable is move-only because it owns a reference to the state.
            FutureAwaitable(const FutureAwaitable&) = delete;
            FutureAwaitable& operator=(const FutureAwaitable&) = delete;

            /// Transfers the reference from the source.
            /// @param other Source, which no longer owns the state after the move.
            FutureAwaitable(FutureAwaitable&& other) noexcept
                : m_state(std::exchange(other.m_state, nullptr)),
                  m_executor(other.m_executor),
                  m_awaited(other.m_awaited) {}

            /// Releases the previous reference and takes the source's reference.
            /// @param other Source; self-assignment is safe.
            /// @return Reference to this awaitable.
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

            /// Claims the single consumption of the result.
            /// @return false if already awaited or if no state is present.
            [[nodiscard]] bool claimAwait() noexcept {
                if (m_awaited || !m_state || !m_state->claimConsumer()) {
                    return false;
                }
                m_awaited = true;
                return true;
            }

            /// Creates an awaiter with the specified parent-task cancellation flag.
            /// @param cancelFlag Non-owning pointer or nullptr.
            /// @return Move-only awaiter holding its own state reference.
            FutureAwaiter prepareAwaiter(std::atomic<bool>* cancelFlag) noexcept {
                return FutureAwaiter{m_state, m_executor, cancelFlag};
            }

            /// Awaits a temporary awaitable; reusing it terminates the process.
            FutureAwaiter operator co_await() && noexcept {
                if (!claimAwait()) [[unlikely]] {
                    std::terminate();
                }
                return prepareAwaiter(nullptr);
            }
        };

        /// Creates an awaitable configured with a resumption executor.
        /// @param executor Non-owning executor; must remain alive until the wait completes.
        [[nodiscard]] FutureAwaitable prepareAwaitable(IExecutor* executor) noexcept {
            return FutureAwaitable{m_state, executor};
        }

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
        [[nodiscard]] std::atomic<bool>* cancellationFlag() noexcept {
            return m_state ? m_state->cancellationFlag() : nullptr;
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
