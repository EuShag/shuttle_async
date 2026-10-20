#pragma once
#include <cassert>
#include <coroutine>
#include <atomic>
#include <expected>
#include <concepts>
#include <optional>
#include <type_traits>
#include <async/async_shared_state.hpp>
#include <async/error.hpp>
#include <async/mi_allocator.hpp>
#include <async/coro_scope.hpp>
#include <async/awaiters.hpp>

namespace shuttle::async {

    /**
     * @brief Marker for a cooperative cancellation check point.
     *
     * In an AsyncTask coroutine, `co_await cancellationPoint` returns
     * AsyncResult<void> whose flag reflects the task's current cancellation
     * flag; it does not suspend the coroutine.
     */
    struct CancellationPoint {};
    /// Global immutable object for `co_await cancellationPoint`.
    inline constexpr CancellationPoint cancellationPoint{};

    template <typename T>
    class CancellationToken;

    class CoroScope;

    /**
     * @brief Helper policy for return_value/return_void in an AsyncTask promise.
     * @tparam T Task result type.
     * @tparam Promise Associated coroutine promise.
     * @tparam bool Selects the void specialization when T == void.
     */
    template <typename T, typename Promise, bool = std::same_as<T, void>>
    struct AsyncTaskReturnOps;

    /// Implements result storage and child-scope completion for T != void.
    template <typename T, typename Promise>
    struct AsyncTaskReturnOps<T, Promise, false> {
        /// Moves the return value into shared state.
        /// @param value Value returned by the coroutine.
        void return_value(T&& value) noexcept(std::is_nothrow_move_constructible_v<T>) {
            auto& promise = static_cast<Promise&>(*this);
            promise.m_promise.storeValue(std::move(value));
            if (promise.m_coroScope) [[likely]] promise.m_coroScope->joinTask();
        }

        /// Copies the return value into shared state when T is copyable.
        /// @param value Value returned by the coroutine.
        void return_value(const T& value) noexcept(std::is_nothrow_copy_constructible_v<T>)
            requires std::copy_constructible<T> {
            auto& promise = static_cast<Promise&>(*this);
            promise.m_promise.storeValue(value);
            if (promise.m_coroScope) [[likely]] promise.m_coroScope->joinTask();
        }
    };

    /// Implements completion without a payload for AsyncTask<void>.
    template <typename Promise>
    struct AsyncTaskReturnOps<void, Promise, true> {
        /// Completes the coroutine scope without storing a value.
        void return_void() noexcept {
            auto& promise = static_cast<Promise&>(*this);
            if (promise.m_coroScope) [[likely]] promise.m_coroScope->joinTask();
        }
    };

    /**
     * @brief Move-only result of a coroutine operation.
     *
     * AsyncTask owns a coroutine handle and a Future. When it is destroyed while
     * it still owns the handle, its destructor only sets the cancellation flag:
     * this is cooperative cancellation, not synchronous destruction of a running
     * frame. When awaited, the handle is transferred to the awaiter, and the
     * frame is destroyed by FinalAwaiter. Launch modes are Immediate (the scope
     * runs the body during creation), Async (the scope submits it to an executor),
     * and Lazy (the task waits for start/co_await).
     *
     * Cancellation is observed through cancellationPoint and awaiters; an
     * arbitrary coroutine body is not interrupted automatically. Creation and
     * launch errors are available as Error/Status, and successful awaiting
     * returns AsyncResult<T>.
     *
     * @tparam T Return type; defaults to void.
     */
    template <typename T = void>
    class [[nodiscard]] AsyncTask final {
    public:
        /**
         * @brief Coroutine promise that connects the frame, scope, and shared state.
         *
         * State is allocated separately using MiAllocator. If allocation fails,
         * get_return_object returns an errored AsyncTask and the body is not run.
         */
        struct promise_type : AsyncTaskReturnOps<T, promise_type> {
            /// Non-owning parent scope; the frame must complete before the scope.
            CoroScope* m_coroScope{ nullptr };
            /// Producer reference to shared state.
            AsyncPromise<T> m_promise;
            /// Consumer reference returned from the coroutine.
            AsyncFuture<T> m_future;
            /// Flag indicating failure to create shared state.
            bool m_stateCreationFailed{ false };

            /// Initializes state for a coroutine whose first parameter is an object followed by a scope.
            /// @tparam TObj Type of the first coroutine parameter.
            /// @tparam TArgs Types of the remaining coroutine parameters.
            /// @param scope Scope stored in the frame.
            template <typename TObj, typename... TArgs>
            promise_type(TObj&&, CoroScope& scope, TArgs&...) noexcept
                : m_coroScope(&scope) {
                initializeState(scope.canImmediateLaunched());
            }

            /// Initializes state for a coroutine whose first parameter is CoroScope.
            /// @tparam TArgs Types of the additional coroutine parameters.
            /// @param scope Scope stored in the frame.
            template <typename... TArgs>
            promise_type(CoroScope& scope, TArgs&...) noexcept
                : m_coroScope(&scope) {
                initializeState(scope.canImmediateLaunched());
            }

            /// Creates a promise without a scope, used for coroutines without a scope parameter.
            promise_type() noexcept = default;

            /// Creates an AsyncTask from the state and registers the coroutine handle with the scope.
            /// @return allocation_failure if state allocation failed.
            [[nodiscard]] AsyncTask get_return_object() noexcept {
                auto const coroHandle = std::coroutine_handle<promise_type>::from_promise(*this);
                if (m_stateCreationFailed) [[unlikely]] {
                    return AsyncTask{Error::allocation_failure, coroHandle};
                }
                if (m_coroScope) [[likely]] {
                    m_coroScope->registerCoroHandle(coroHandle);
                }
                return AsyncTask{std::move(m_future), coroHandle};
            }

            /// Coroutine-frame allocation failure produces an errored AsyncTask.
            [[nodiscard]] static AsyncTask get_return_object_on_allocation_failure() noexcept {
                return AsyncTask{ Error::allocation_failure };
            }

            /// Selects initial suspension based on the launch mode and allocation result.
            [[nodiscard]] auto initial_suspend() const noexcept {
                return InitialAwaiter{m_coroScope, m_stateCreationFailed};
            }
            /// Creates a finalizer that publishes completion before destroying the frame.
            [[nodiscard]] auto final_suspend() const noexcept {
                return FinalAwaiter{
                    m_coroScope,
                    m_promise.stateAddress(),
                    &AsyncPromise<T>::retainState,
                    &AsyncPromise<T>::releaseState,
                    &AsyncPromise<T>::completeState
                };
            }

            /// Exceptions from the user coroutine body are fatal.
            void unhandled_exception() const noexcept { std::terminate(); }

            /// Adds the cancellation flag to a supported awaitable.
            /// @tparam Awaitable One-shot awaitable with claimAwait/prepareAwaiter.
            /// @param awaitable Awaitable whose consumption is transferred to the promise.
            /// @return Prepared awaiter.
            /// Reusing an awaitable is a fatal error.
            template <typename Awaitable>
                requires requires(Awaitable& awaitable, std::atomic<bool>* flag) {
                    awaitable.claimAwait();
                    awaitable.prepareAwaiter(flag);
                }
            [[nodiscard]] auto await_transform(Awaitable&& awaitable) noexcept {
                if (!awaitable.claimAwait()) [[unlikely]] {
                    std::terminate();
                }
                // Пробрасываем текущий экзекьютор из TLS и общий флаг отмены задачи.
                return awaitable.prepareAwaiter(m_promise.cancellationFlag());
            }

            /// Configures the nested task to use the current thread's pinned executor.
            /// @tparam U Result type of the nested task.
            /// @param task Nested task consumed by the sole consumer.
            /// @return Awaiter that propagates this task's cancellation flag.
            template <typename U>
            [[nodiscard]] AsyncFuture<U>::FutureAwaiter await_transform(AsyncTask<U>& task) noexcept {
                return task.getAwaiter(
                    IExecutor::getThreadPinnedExecutor(), m_promise.cancellationFlag());
            }

            /// Configures the nested task to use the current thread's pinned executor.
            /// @tparam U Result type of the nested task.
            /// @param task Temporary or moved task.
            /// @return Awaiter that propagates this task's cancellation flag.
            template <typename U>
            [[nodiscard]] AsyncFuture<U>::FutureAwaiter await_transform(AsyncTask<U>&& task) noexcept {
                return task.getAwaiter(
                    IExecutor::getThreadPinnedExecutor(), m_promise.cancellationFlag());
            }

            /// Checks the cancellation flag without suspending.
            struct CancellationAwaiter {
                /// Non-owning flag of the parent task.
                std::atomic<bool>* flag;
                /// Always ready: cancellation is only read.
                [[nodiscard]] bool await_ready() const noexcept { return true; }
                /// Unused because await_ready is always true.
                void await_suspend(std::coroutine_handle<>) const noexcept {}
                /// Returns a snapshot of the cancellation flag.
                [[nodiscard]] AsyncResult<void> await_resume() const noexcept {
                    return AsyncResult<void>{.isCancelled = flag->load(std::memory_order_acquire)};
                }
            };

            /// Converts a cancellationPoint marker to a CancellationAwaiter.
            [[nodiscard]] CancellationAwaiter await_transform(CancellationPoint) noexcept {
                return CancellationAwaiter{m_promise.cancellationFlag()};
            }

            /// Forwards other unsupported awaitables without transformation.
            /// @tparam Awaitable Type of the arbitrary awaitable.
            /// @param awaitable Object forwarded with its value category preserved.
            /// @return The original object or a reference to it.
            template <typename Awaitable>
                requires (!std::same_as<std::remove_cvref_t<Awaitable>, CancellationPoint>)
                    && (!requires(std::remove_cvref_t<Awaitable>& awaitable, std::atomic<bool>* flag) {
                        awaitable.claimAwait();
                        awaitable.prepareAwaiter(flag);
                    })
            decltype(auto) await_transform(Awaitable&& awaitable) noexcept {
                return std::forward<Awaitable>(awaitable);
            }

            /**
             * @brief Awaiter that switches the current coroutine to a specified executor.
             *
             * Always goes through await_suspend. If cancellation is already set,
             * the executor is null, or execute() fails, it continues the current
             * handle on the current thread; on success, it returns the current
             * thread's dispatcher.
             */
            struct SwitchExecutorAwaiter final {
                /// Non-owning target executor.
                IExecutor* targetExecutor{ nullptr };
                /// Non-owning cancellation flag of the parent task.
                std::atomic<bool>* parentCancelledFlag{ nullptr };

                /// Always false: the switch attempt is performed in await_suspend.
                [[nodiscard]] constexpr bool await_ready() const noexcept {
                    // Мы ВСЕГДА приостанавливаемся, чтобы честно выполнить
                    // маршаллинг задачи в очередь нового исполнителя
                    return false;
                }

                /// Enqueues the coroutine handle on the executor or continues it immediately.
                /// @param currentCoro Handle of the current coroutine.
                /// @return Dispatcher handle on successful enqueue; otherwise, currentCoro.
                std::coroutine_handle<> await_suspend(std::coroutine_handle<> currentCoro) noexcept {
                    // FAST-PATH ЗАЩИТА: Если корутина отменена прямо перед прыжком —
                    // вообще никуда не летим, мгновенно просыпаемся на текущем месте для эвакуации!
                    if (parentCancelledFlag && parentCancelledFlag->load(std::memory_order_acquire)) {
                        return currentCoro;
                    }

                    if (targetExecutor) [[likely]] {
                        // Толкаем текущий хэндл в лок-фри воронку нового экзекьютора!
                        // Метод execute() сам разберется, пушить в деку или на Шаг 0 воркера.
                        if (!targetExecutor->execute(currentCoro)) [[unlikely]] {
                            return currentCoro;
                        }
                    } else {
                        return currentCoro;
                    }

                    // Текущий физический поток ОС полностью свободен, уходит в анабиоз или за задачами!
                    return IExecutor::getCurrentDispatcherCoroutine();
                }

                /// Returns the cancellation state after switching or immediate continuation.
                [[nodiscard]] AsyncResult<void> await_resume() const noexcept {
                    bool const cancelled = parentCancelledFlag ? parentCancelledFlag->load(std::memory_order_acquire) : false;

                    // Возвращаем ультралегкий флаг. Если false — пользователь прервет выполнение.
                    return AsyncResult<void>{ .isCancelled = cancelled };
                }
            };

            /// Allows `co_await executor` to switch executors.
            /// @param explicitExecutor Non-owning target executor or nullptr.
            /// @return Switch awaiter; nullptr means immediate continuation.
            [[nodiscard]] auto await_transform(IExecutor* explicitExecutor) noexcept {
                return SwitchExecutorAwaiter{
                    .targetExecutor = explicitExecutor,
                    .parentCancelledFlag = m_promise.cancellationFlag()
                };
            }

            /// Creates allocator-owned shared state; failure is recorded for the return object.
            /// @param started Initial state of the started flag.
            void initializeState(bool started) noexcept {
                auto state = makeAsyncState<T>(MiAllocator<std::byte>{}, started);
                if (!state) [[unlikely]] {
                    m_stateCreationFailed = true;
                    return;
                }
                m_promise = std::move(state->promise);
                m_future = std::move(state->future);
            }

            /// Allocates the coroutine frame using MiAllocator.
            [[nodiscard]] void* operator new(std::size_t size) noexcept {
                return MiAllocator<char>{}.allocate(size);
            }
            /// Frees the coroutine frame allocated by operator new.
            void operator delete(void* ptr, std::size_t) noexcept {
                MiAllocator<char>{}.deallocate(static_cast<char*>(ptr));
            }
        };

        /// Creates a task from a Future and coroutine frame, taking ownership of both.
        /// @param future Consumer reference to shared state.
        /// @param handle Owned coroutine frame.
        AsyncTask(AsyncFuture<T> future, std::coroutine_handle<promise_type> handle) noexcept
            : m_future{std::move(future)}, m_handle{handle} {}
        /// Creates an empty task in an error state.
        /// @param error Reason the task is unavailable.
        AsyncTask(Error error) noexcept
            : m_error{error} {}
        /// Creates an errored task that still owns the frame and will destroy it.
        /// @param error Error reason.
        /// @param handle Frame to destroy when the object is destroyed.
        AsyncTask(Error error, std::coroutine_handle<promise_type> handle) noexcept
            : m_handle{handle}, m_error{error} {}

        /**
         * @brief Requests cancellation when the task is destroyed while it still owns it.
         *
         * If the task contains an error, the frame is destroyed immediately;
         * otherwise, only the shared cancellation flag is set. A running frame
         * remains alive until its own finalization; this destructor does not
         * explicitly destroy the frame.
         */
        ~AsyncTask() noexcept {
            if (m_handle) {
                if (m_error) {
                    m_handle.destroy();
                } else {
                    m_future.cancel();
                }
            }
        }

        /**
         * @brief Sets the cooperative cancellation flag and transfers the task to a token.
         * @return Token for waiting for completion; the cancellation request
         *         alone does not signal the Future or interrupt the coroutine body.
         */
        [[nodiscard]] CancellationToken<T> cancel() noexcept;

        /// @return Current cancellation flag state; an empty task is considered cancelled.
        [[nodiscard]] bool isCanceled() const noexcept {
            return m_future.isCanceled();
        }

        /// Copying is disabled because the task uniquely owns its coroutine handle.
        AsyncTask(const AsyncTask&) = delete;
        AsyncTask& operator=(const AsyncTask&) = delete;

        /// Transfers ownership of the handle and state, leaving the source empty.
        /// @param other Source, which no longer owns the frame/state after the move.
        AsyncTask(AsyncTask&& other) noexcept
            : m_future(std::move(other.m_future)),
              m_handle(std::exchange(other.m_handle, nullptr)),
              m_error{other.m_error} {
            other.m_error.reset();
        }

        /// Releases the previous Future and takes ownership from the source.
        /// The previous handle is not explicitly destroyed and its cancellation
        /// flag is unchanged.
        /// @param other Ownership source; self-assignment is safe.
        /// @return Reference to this AsyncTask.
        AsyncTask& operator=(AsyncTask&& other) noexcept {
            if (this != &other) {
                m_future = std::move(other.m_future);
                m_handle = std::exchange(other.m_handle, nullptr);
                m_error = other.m_error;
                other.m_error.reset();
            }
            return *this;
        }

        /**
         * @brief Starts the task if needed and prepares to await it on an executor.
         * @param explicitExecutor Non-owning executor used to resume the waiter;
         *                         nullptr is allowed only if the result is ready.
         * @return Awaitable with its own reference to the state.
         * @note The current TLS context's pinned executor is used to start the task;
         *       explicitExecutor is used to resume the waiting caller.
         * @note If startup fails, the await path may resume the coroutine on the current thread.
         * @note Thread safety: does not synchronize concurrent moves/awaits of one object.
         * @note The frame handle is removed from AsyncTask so its destructor will
         *       not spuriously cancel it after ownership is transferred to the awaiter.
         */
        [[nodiscard]] AsyncFuture<T>::FutureAwaitable get(IExecutor* explicitExecutor) noexcept {
            requireValid();
            startForAwait();
            if (explicitExecutor == nullptr && !m_future.isSignaled()) [[unlikely]] {
                std::terminate();
            }
            /// Safely transfer the handle to the awaiter, clearing it locally via std::exchange.
            /// The ~AsyncTask() destructor will see nullptr and avoid triggering a false cancellation.
            std::exchange(m_handle, nullptr);
            return m_future.prepareAwaitable(explicitExecutor);
        }

        /**
         * @brief Prepares to await using the pinned executor from the current TLS context.
         * @return Awaitable for the result.
         * @note The pinned executor is used to start the task; if enqueueing fails,
         *       startForAwait resumes the coroutine directly on the current thread.
         *       If no executor is available and the task is incomplete, terminate is called.
         * @note Thread safety: does not synchronize concurrent use of this object.
         */
        [[nodiscard]] AsyncFuture<T>::FutureAwaitable get() noexcept {
            requireValid();
            startForAwait();
            IExecutor* const executor = IExecutor::getThreadPinnedExecutor();
            if (executor == nullptr && !m_future.isSignaled()) [[unlikely]] {
                std::terminate();
            }
            std::exchange(m_handle, nullptr);
            return m_future.prepareAwaitable(executor);
        }

        /**
         * @brief Creates an awaiter for nested co_await with the parent's cancellation flag.
         * @param executor Non-owning executor used to resume the waiter.
         * @param cancellationFlag Parent coroutine's cancellation flag, or nullptr.
         * @return Awaiter that consumes the result exactly once.
         * @pre The task is valid; a repeated consumer calls terminate.
         * @note If startup fails, the coroutine may be resumed synchronously on
         *       the current thread before this method returns.
         */
        [[nodiscard]] AsyncFuture<T>::FutureAwaiter getAwaiter(
            IExecutor* executor,
            std::atomic<bool>* cancellationFlag) noexcept {
            requireValid();
            startForAwait();
            if (executor == nullptr && !m_future.isSignaled()) [[unlikely]] {
                std::terminate();
            }
            if (!m_future.claimConsumer()) [[unlikely]] {
                std::terminate();
            }
            std::exchange(m_handle, nullptr);
            return m_future.prepareAwaitable(executor).prepareAwaiter(cancellationFlag);
        }

        /// Equivalent to `co_await get()` using the current thread's pinned executor.
        /// @return FutureAwaitable for the task result.
        auto operator co_await() noexcept {
            return get(); // Просто перенаправляем в дефолтный TaskAwaiter
        }

        /**
         * @brief Enqueues the task on the specified executor if it has not started yet.
         * @param executor Executor used to start the task; nullptr returns executor_unavailable.
         * @return Success if already signaled/started; otherwise allocation_failure,
         *         invalid_coroutine_handle, executor_unavailable, or an executor enqueue error.
         * @note Thread safety: start is deduplicated atomically in shared state;
         *       ownership of AsyncTask and its coroutine handle is not thread-safe.
         */
        Status start(IExecutor* executor) noexcept {
            if (m_future.isSignaled()) {
                return {};
            }
            if (!m_future.hasState()) [[unlikely]] {
                return std::unexpected(m_error.value_or(Error::allocation_failure));
            }
            if (!m_handle || m_handle == std::noop_coroutine()) [[unlikely]] {
                return std::unexpected(m_error.value_or(Error::invalid_coroutine_handle));
            }
            if (!m_future.beginStart()) {
                return {};
            }
            if (executor == nullptr) [[unlikely]] {
                m_future.resetStarted();
                return std::unexpected(Error::executor_unavailable);
            }
            auto result = executor->execute(m_handle);
            if (!result) {
                m_future.resetStarted();
            }
            return result;
        }

        /// Starts the task on the current thread's pinned executor.
        /// @return Result of start(IExecutor*); without a TLS executor, returns an
        ///         error if the task has not completed.
        Status start() noexcept {
            return start(IExecutor::getThreadPinnedExecutor());
        }

        /// @return true if the handle is non-null and no error is present;
        ///         does not guarantee that the frame has not completed.
        [[nodiscard]] bool has_value() const noexcept { return m_handle != nullptr && !m_error.has_value(); }
        /// Returns a non-owning handle; requires has_value() and an incomplete frame.
        /// @return Handle to the frame owned by the task.
        /// @warning After immediate completion, the handle may no longer be valid.
        std::coroutine_handle<> get_handle() noexcept {
            assert(has_value());
            return m_handle;
        }
        /// Returns the creation/enqueue error, if any.
        [[nodiscard]] std::optional<Error> getError() const noexcept { return m_error; }

    private:
        template <typename>
        friend class BlockingTask;
        template <typename>
        friend class CancellationToken;

        // Precondition for public operations that require an existing frame.
        void requireValid() const noexcept {
            if (!has_value()) [[unlikely]] {
                std::terminate();
            }
        }

        // Shared-state invariant: the consumer may extract the value exactly once.
        void requireSingleConsumer() noexcept {
            if (!m_future.claimConsumer()) [[unlikely]] {
                std::terminate();
            }
        }

        // The await path starts on the pinned executor; if enqueueing fails,
        // resume the frame synchronously on the current thread.
        void startForAwait() noexcept {
            if (m_future.isSignaled()) {
                return;
            }
            auto result = start(IExecutor::getThreadPinnedExecutor());
            if (!result) {
                m_future.markStarted();
                m_handle.resume();
            }
        }

        /// Future reference to the shared result.
        AsyncFuture<T> m_future{};
        /// Owning frame handle, cleared when the task completes or is cancelled.
        std::coroutine_handle<promise_type> m_handle{ nullptr };
        /// Creation or startup error; absence of an error does not imply completion.
        std::optional<Error> m_error;
    };

    /**
     * @brief Move-only representation of a cancelled task.
     *
     * Owns a moved AsyncTask and awaits its result exactly once. Cancellation is
     * cooperative; this token's awaiter forcibly sets isCancelled in AsyncResult,
     * regardless of the producer's value.
     *
     * @tparam T Result type of the wrapped task.
     */
    template <typename T>
    class [[nodiscard]] CancellationToken final {
    public:
        /// Takes ownership of the task and its coroutine handle.
        explicit CancellationToken(AsyncTask<T>&& task) noexcept
            : m_task{std::move(task)} {}

        /// Copying is disabled because the token owns a one-shot task.
        CancellationToken(const CancellationToken&) = delete;
        CancellationToken& operator=(const CancellationToken&) = delete;
        /// Transfers ownership of the task.
        CancellationToken(CancellationToken&&) noexcept = default;
        /// Transfers ownership, releasing the previously held task.
        CancellationToken& operator=(CancellationToken&&) noexcept = default;

        /// @return true if the wrapped task contains a valid handle.
        [[nodiscard]] bool has_value() const noexcept {
            return m_task.has_value();
        }

        /// @return Task creation or startup error, if any.
        [[nodiscard]] std::optional<Error> getError() const noexcept {
            return m_task.getError();
        }

        /// Awaiter that delegates scheduling to FutureAwaiter and marks the result cancelled.
        struct Awaiter {
            /// Inner awaiter that retains a reference to the state.
            AsyncFuture<T>::FutureAwaiter m_taskAwaiter;

            /// @return true if the task has already been signaled.
            [[nodiscard]] bool await_ready() const noexcept {
                return m_taskAwaiter.await_ready();
            }

            /// Delegates waiter registration and scheduling to the shared state.
            std::coroutine_handle<> await_suspend(std::coroutine_handle<> handle) noexcept {
                return m_taskAwaiter.await_suspend(handle);
            }

            /// Returns the result with the cancellation flag forcibly set.
            [[nodiscard]] auto await_resume() noexcept {
                auto result = m_taskAwaiter.await_resume();
                result.isCancelled = true;
                return result;
            }
        };

        /**
         * @brief Prepares a one-shot awaiter for the wrapped task.
         * @param resumeExecutor Non-owning executor used to resume the waiter.
         * @return Awaiter; nullptr is allowed only if completion has already been published.
         */
        [[nodiscard]] Awaiter get(IExecutor* resumeExecutor = IExecutor::getThreadPinnedExecutor()) noexcept {
            return Awaiter{m_task.getAwaiter(resumeExecutor, nullptr)};
        }

        /// Awaits using the current thread's pinned executor.
        Awaiter operator co_await() & noexcept {
            return get();
        }

        /// Uses the same waiting path as the lvalue overload.
        Awaiter operator co_await() && noexcept {
            return operator co_await();
        }

    private:
        /// Sole task awaited by synchronous get().
        AsyncTask<T> m_task;
    };

    /**
     * @brief Requests cooperative cancellation and transfers the task to a token.
     * @return Move-only token for awaiting task completion.
     */
    template <typename T>
    CancellationToken<T> AsyncTask<T>::cancel() noexcept {
        m_future.cancel();
        return CancellationToken<T>{std::move(*this)};
    }

    /**
     * @brief AsyncTask wrapper whose get() blocks the calling OS thread.
     *
     * Used by the JoinableScope::launch overload with explicit options. start()
     * remains asynchronous; get() installs a BlockingExecutor in the current
     * thread's TLS and waits for shared-state signaling. For non-void T, get()
     * returns an rvalue reference to the value in the state, not an owning value.
     * This interface does not check cancellation or payload presence before
     * extracting the result.
     *
     * @tparam T Result type of the wrapped task.
     */
    template <typename T>
    class BlockingTask {
        /// Signaling executor whose lifetime covers synchronous get().
        BlockingExecutor m_executor{};
    public:
        /// Takes ownership of an AsyncTask.
        BlockingTask(AsyncTask<T>&& task) noexcept : m_task{std::move(task)} {}

        /// Starts the task on the supplied executor.
        /// @param executor Non-owning executor.
        /// @return Task startup status.
        Status start(IExecutor* executor) noexcept {
            return m_task.start(executor);
        }
        /// Starts the task on the current thread's pinned executor.
        /// @return Task startup status.
        Status start() noexcept {
            return m_task.start();
        }

        /// Requests cancellation and transfers ownership of the task to a token.
        [[nodiscard]] CancellationToken<T> cancel() noexcept {
            return m_task.cancel();
        }

        /// @return true if the wrapped task's coroutine handle is valid.
        [[nodiscard]] bool has_value() const noexcept {
            return m_task.has_value();
        }

        /// @return The task's recorded error, if any.
        [[nodiscard]] std::optional<Error> getError() const noexcept {
            return m_task.getError();
        }

        /**
         * @brief Blocks the current OS thread until the shared state is signaled.
         * @return void for T == void; otherwise, an rvalue reference to the stored T.
         * @pre The task is valid and a consumer is available; an executor must
         *      be configured for asynchronous completion.
         * @warning For non-void tasks, the reference is non-owning and becomes
         *          invalid when the shared state is destroyed. Do not extract
         *          a result from a cancelled task with no value.
         */
                decltype(auto) get() {
            m_task.requireValid();
            m_task.requireSingleConsumer();
            m_task.startForAwait();
            auto threadContextGuard = m_executor.initializeThreadContext();

            // 1. Obtain the awaitable object by passing the address of our internal m_executor.
            // This directly accesses the internal state of the AsyncTask.
            auto awaitable = m_task.m_future.prepareAwaitable(&m_executor);
            auto* state = awaitable.m_state;

            // -----------------------------------------------------------------
            // FAST-PATH STAGE 1: Data is ALREADY available (Producer finished before get)
            // -----------------------------------------------------------------
            if (state->isSignaled()) {
                if constexpr (std::same_as<T, void>) return;
                else return state->extractValue();
            }

            // -----------------------------------------------------------------
            // FAST-PATH STAGE 2: Data race with the producer during registration.
            // If tryRegisterWaiter returns false, it implies that while we were
            // writing pointers, the producer IMMEDIATELY transitioned the state to Signaled!
            // -----------------------------------------------------------------
            if (!state->tryRegisterWaiter(std::noop_coroutine(), &m_executor)) {
                if constexpr (std::same_as<T, void>) return;
                else return state->extractValue();
            }

            // -----------------------------------------------------------------
            // SLOW-PATH STAGE 3: Data is absent, waiter is successfully registered.
            // Force the physical OS thread into deep kernel-level suspension via futex/WaitOnAddress.
            // Protection against spurious wake-ups (while loop) is already handled inside waitForSignal!
            // -----------------------------------------------------------------
            m_executor.waitForSignal();

            // The thread has woken up; data is guaranteed to be available.
            // Extract the r-value reference without performing a single copy operation!
            if constexpr (std::same_as<T, void>) return;
            else return state->extractValue();
        }

    private:
        /// Sole task awaited by synchronous get().
        AsyncTask<T> m_task;
    };

} // namespace shuttle::async
