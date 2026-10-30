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
#include <async/awaiters.hpp>

#include "i_cancellable.hpp"

namespace shuttle::async {

    class CoroScope;

    /**
     * @brief Ultra-lightweight awaiter for cooperative time-slicing (yield).
     *
     * Re-enqueues the current coroutine into the back of the local or current
     * executor's queue to let other same-priority tasks execute.
     */
    struct YieldAwaiter final {
        /// Always false: we must suspend to let the scheduler breath.
        [[nodiscard]] constexpr bool await_ready() const noexcept {
            return false;
        }

        /// Enqueues the coroutine handle to the current thread's executor.
        /// @param currentCoro Handle of the current coroutine.
        /// @return Dispatcher handle to return control to the host worker loop.
        bool await_suspend(std::coroutine_handle<> currentCoro) noexcept {
            if (IExecutor* currentExecutor = IExecutor::getThreadPinnedExecutor()) [[likely]] {
                // Пушим в локальную деку/очередь текущего воркера.
                // Это работает со скоростью света, так как кэш-лайн горячий и принадлежит этому же ядру!
                if(!currentExecutor->execute(currentCoro).has_value()) std::terminate();
            } else {
                // Аварийный путь: если вызвали yield вне пула (например, на потоке Win32 окна)
                return false;
            }

            // Освобождаем текущий поток ОС, отдавая управление главному циклу воркера (диспетчеру)
            return true;
        }

        /// Yield doesn't enforce immediate cancellation drop by default,
        /// but returns a lightweight status if the loop wants to check it manually.
        constexpr void await_resume() const noexcept {}
    };

    inline YieldAwaiter yield() {
        return {};
    }

    template <typename TAwaiter>
    class NonCancellableAwaiter final {
    public:
        // Конструктор забирает оригинальный авайтер, а ноду настраивает на месте
        explicit NonCancellableAwaiter(TAwaiter awaiter) noexcept
            : m_awaiter{std::move(awaiter)}
            , m_dummyNode{
                .handle = nullptr,
                .executor = nullptr,
                .nodeState = { NodeState::PENDING },
                // Привязываем указатель на наш вечный false
                .parentCancelledFlag = false
            }
        {}

        // Запрещаем любое копирование и перемещение, так как нода содержит атомики
        // и её адрес в стеке корутины должен быть намертво зафиксирован!
        NonCancellableAwaiter(const NonCancellableAwaiter&) = delete;
        NonCancellableAwaiter& operator=(const NonCancellableAwaiter&) = delete;
        NonCancellableAwaiter(NonCancellableAwaiter&&) = delete;
        NonCancellableAwaiter& operator=(NonCancellableAwaiter&&) = delete;

        // 1. Игнорируем внешнюю ноду промиса, передаем адрес НАШЕЙ локальной m_dummyNode
        [[nodiscard]] bool await_ready(WaitNode*) noexcept(noexcept(m_awaiter.await_ready(nullptr))) {
            return m_awaiter.await_ready(&m_dummyNode);
        }

        // 2. Оригинальный авайтер (например, ожидание фенса) честно запишет свой handle/executor
        // в нашу локальную m_dummyNode и уйдет спать в футекс без риска экстренной отмены
        bool await_suspend(std::coroutine_handle<> coro, WaitNode*) noexcept {
            return m_awaiter.await_suspend(coro, &m_dummyNode);
        }

        // 3. Возвращаем чистый результат без флага отмены
        [[nodiscard]] auto await_resume(WaitNode*) noexcept(noexcept(m_awaiter.await_resume(nullptr))) {
            return m_awaiter.await_resume(&m_dummyNode);
        }

    private:
        TAwaiter m_awaiter;
        WaitNode m_dummyNode; // Нода живет на стеке корутины внутри эвейтера!
    };

    // Наш фабричный хелпер. Благодаря Guaranteed Copy Elision в C++17/20,
    // компилятор соберет NonCancellableAwaiter СРАЗУ в целевой точке await_transform,
    // минуя вызовы move-конструкторов, что обходит ограничение атомиков!
    template <typename TAwaiter>
    [[nodiscard]] auto nonCancellable(TAwaiter&& awaiter) noexcept {
        return NonCancellableAwaiter<std::remove_cvref_t<TAwaiter>>{std::forward<TAwaiter>(awaiter)};
    }

    template <typename T, typename ResultType = void>
    concept CancellableAwaiter = requires(T awaiter, std::coroutine_handle<> coro, WaitNode* pWaitNode) {
        // await_ready принимает флаг и возвращает bool
        { awaiter.await_ready(pWaitNode) } -> std::same_as<bool>;

        // await_suspend ПРИНИМАЕТ хэндл И флаг отмены, возвращая хэндл для Symmetric Transfer
        { awaiter.await_suspend(coro, pWaitNode) } -> std::same_as<bool>;

        // await_resume принимает флаг и возвращает итоговый результат
        { awaiter.await_resume(pWaitNode) } -> std::same_as<ResultType>;
    };

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
    class [[nodiscard]] AsyncTask final : public ICancellable {
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
                    m_coroScope
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

            /**
             * @brief Транспортный враппер, передающий флаг отмены во все стадии оригинального авайтера.
             */
            template <typename TAwaiter>
            class TransformedAwaiter final {
            public:
                TransformedAwaiter(TAwaiter awaiter, WaitNode* pWaitNode) noexcept
                    : m_awaiter{std::move(awaiter)} , m_pWaitNode{pWaitNode} {}

                [[nodiscard]] bool await_ready() noexcept(noexcept(m_awaiter.await_ready(m_pWaitNode))) {
                    return m_awaiter.await_ready(m_pWaitNode);
                }

                // Передаем хэндл корутины и флаг отмены в оригинальный авайтер
                std::coroutine_handle<> await_suspend(std::coroutine_handle<> coro) noexcept {
                    return m_awaiter.await_suspend(coro, m_pWaitNode) ? IExecutor::getCurrentDispatcherCoroutine() : coro;
                }

                [[nodiscard]] auto await_resume() noexcept(noexcept(m_awaiter.await_resume(m_pWaitNode))) {
                    return m_awaiter.await_resume(m_pWaitNode);
                }

            private:
                TAwaiter  m_awaiter;
                WaitNode* m_pWaitNode;
            };

            template <typename Awaiter>
                auto await_transform(Awaiter&& awaiter) noexcept {
                return TransformedAwaiter<std::remove_cvref_t<Awaiter>>(
                    std::forward<Awaiter>(awaiter),
                    m_promise.waitNode()
                );
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
        ~AsyncTask() noexcept override {
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
        void cancel() noexcept override;

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
        [[nodiscard]] AsyncFuture<T>::FutureAwaiter get(IExecutor* explicitExecutor) noexcept {
            requireValid();
            startForAwait();
            if (explicitExecutor == nullptr && !m_future.isSignaled()) [[unlikely]] {
                std::terminate();
            }
            /// Safely transfer the handle to the awaiter, clearing it locally via std::exchange.
            /// The ~AsyncTask() destructor will see nullptr and avoid triggering a false cancellation.
            std::exchange(m_handle, nullptr);
            return m_future.getAwaiter(explicitExecutor);
        }

        /**
         * @brief Prepares to await using the pinned executor from the current TLS context.
         * @return Awaitable for the result.
         * @note The pinned executor is used to start the task; if enqueueing fails,
         *       startForAwait resumes the coroutine directly on the current thread.
         *       If no executor is available and the task is incomplete, terminate is called.
         * @note Thread safety: does not synchronize concurrent use of this object.
         */
        [[nodiscard]] AsyncFuture<T>::FutureAwaiter get() noexcept {
            requireValid();
            startForAwait();
            IExecutor* const executor = IExecutor::getThreadPinnedExecutor();
            if (executor == nullptr && !m_future.isSignaled()) [[unlikely]] {
                std::terminate();
            }
            std::exchange(m_handle, nullptr);
            return m_future.getAwaiter(executor);
        }

        /**
         * @brief Creates an awaiter for nested co_await with the parent's cancellation flag.
         * @param executor Non-owning executor used to resume the waiter.
         * @return Awaiter that consumes the result exactly once.
         * @pre The task is valid; a repeated consumer calls terminate.
         * @note If startup fails, the coroutine may be resumed synchronously on
         *       the current thread before this method returns.
         */
        [[nodiscard]] AsyncFuture<T>::FutureAwaiter getAwaiter(
            IExecutor* executor) noexcept {
            requireValid();
            startForAwait();
            if (executor == nullptr && !m_future.isSignaled()) [[unlikely]] {
                std::terminate();
            }
            if (!m_future.claimConsumer()) [[unlikely]] {
                std::terminate();
            }
            std::exchange(m_handle, nullptr);
            return m_future.getAwaiter(executor);
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
     * @brief Requests cooperative cancellation and transfers the task to a token.
     * @return Move-only token for awaiting task completion.
     */
    template <typename T>
    void AsyncTask<T>::cancel() noexcept {
        m_future.cancel();
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
        void cancel() noexcept {
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
            auto& state = m_task.m_future.getState(); // Прямой доступ к AsyncSharedState

            // -----------------------------------------------------------------
            // FAST-PATH: Если данные УЖЕ готовы, пролетаем сразу без ухода в сон
            // -----------------------------------------------------------------
            if (state.isSignaled()) {
                if constexpr (std::same_as<T, void>) return;
                else return state.extractValue();
            }

            // -----------------------------------------------------------------
            // SLOW-PATH: Данных нет. Создаем узел ожидания прямо на стеке потока ОС.
            // -----------------------------------------------------------------
            WaitNode node;
            node.handle = std::noop_coroutine();
            node.executor = &m_executor;

            // Вызываем prepare_wait напрямую. Передаем std::noop_coroutine(),
            // так как для физического потока ОС у нас нет корутинного хэндла.
            if (state.prepare_wait(&node)) {
                // Успех! notify() нас не перехватил, мы честно встали в SUSPEND.
                // Безопасно усыпляем физический поток ОС на уровне ядра.
                m_executor.waitForSignal();
            }

            // Поток проснулся (или пролетел гонку), данные гарантированно на месте
            if constexpr (std::same_as<T, void>) return;
            else return state.extractValue();
        }



    private:
        /// Sole task awaited by synchronous get().
        AsyncTask<T> m_task;
    };

} // namespace shuttle::async
