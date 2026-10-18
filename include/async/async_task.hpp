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
     * @if RU
     * @brief Маркер кооперативной точки проверки отмены.
     *
     * В корутине AsyncTask выражение `co_await cancellationPoint` возвращает
     * AsyncResult<void>, чей флаг отражает текущий флаг отмены задачи; оно не
     * приостанавливает корутину.
     * @endif
     * @if EN
     * @brief Marker for a cooperative cancellation check point.
     *
     * In an AsyncTask coroutine, `co_await cancellationPoint` returns
     * AsyncResult<void> whose flag reflects the task's current cancellation
     * flag; it does not suspend the coroutine.
     * @endif
     */
    struct CancellationPoint {};
    /// @if RU
    /// Глобальный неизменяемый объект для `co_await cancellationPoint`.
    /// @endif
    /// @if EN
    /// Global immutable object for `co_await cancellationPoint`.
    /// @endif
    inline constexpr CancellationPoint cancellationPoint{};

    template <typename T>
    class CancellationToken;

    class CoroScope;

    /**
     * @if RU
     * @brief Вспомогательная политика return_value/return_void для promise AsyncTask.
     * @tparam T Тип результата задачи.
     * @tparam Promise Связанный coroutine promise.
     * @tparam bool Выбирает void-специализацию при T == void.
     * @endif
     * @if EN
     * @brief Helper policy for return_value/return_void in an AsyncTask promise.
     * @tparam T Task result type.
     * @tparam Promise Associated coroutine promise.
     * @tparam bool Selects the void specialization when T == void.
     * @endif
     */
    template <typename T, typename Promise, bool = std::same_as<T, void>>
    struct AsyncTaskReturnOps;

    /// @if RU
    /// Реализует сохранение результата и завершение дочернего участка для T != void.
    /// @endif
    /// @if EN
    /// Implements result storage and child-scope completion for T != void.
    /// @endif
    template <typename T, typename Promise>
    struct AsyncTaskReturnOps<T, Promise, false> {
        /// @if RU
        /// Перемещает return value в shared state.
        /// @param value Значение, возвращаемое корутиной.
        /// @endif
        /// @if EN
        /// Moves the return value into shared state.
        /// @param value Value returned by the coroutine.
        /// @endif
        void return_value(T&& value) noexcept(std::is_nothrow_move_constructible_v<T>) {
            auto& promise = static_cast<Promise&>(*this);
            promise.m_promise.storeValue(std::move(value));
            if (promise.m_coroScope) [[likely]] promise.m_coroScope->joinTask();
        }

        /// @if RU
        /// Копирует return value в shared state, если T копируемый.
        /// @param value Значение, возвращаемое корутиной.
        /// @endif
        /// @if EN
        /// Copies the return value into shared state when T is copyable.
        /// @param value Value returned by the coroutine.
        /// @endif
        void return_value(const T& value) noexcept(std::is_nothrow_copy_constructible_v<T>)
            requires std::copy_constructible<T> {
            auto& promise = static_cast<Promise&>(*this);
            promise.m_promise.storeValue(value);
            if (promise.m_coroScope) [[likely]] promise.m_coroScope->joinTask();
        }
    };

    /// @if RU
    /// Реализует завершение без payload для AsyncTask<void>.
    /// @endif
    /// @if EN
    /// Implements completion without a payload for AsyncTask<void>.
    /// @endif
    template <typename Promise>
    struct AsyncTaskReturnOps<void, Promise, true> {
        /// @if RU
        /// Завершает coroutine scope без записи значения.
        /// @endif
        /// @if EN
        /// Completes the coroutine scope without storing a value.
        /// @endif
        void return_void() noexcept {
            auto& promise = static_cast<Promise&>(*this);
            if (promise.m_coroScope) [[likely]] promise.m_coroScope->joinTask();
        }
    };

    /**
     * @if RU
     * @brief Move-only результат корутинной операции.
     *
     * AsyncTask владеет coroutine handle и Future, но его деструктор при
     * уничтожении ещё принадлежащего ему handle только выставляет cancellation
     * flag; это кооперативная отмена, а не синхронное уничтожение работающего
     * frame. При ожидании handle передаётся awaiter-у, а frame уничтожается
     * через FinalAwaiter. Запуск бывает Immediate (scope выполняет при создании),
     * Async (scope ставит в исполнитель) и Lazy (ожидает start/co_await).
     *
     * Отмена наблюдается через cancellationPoint и awaiter-ы; произвольное тело
     * корутины автоматически не прерывается. Ошибки создания/запуска доступны
     * как Error/Status, а успешное ожидание возвращает AsyncResult<T>.
     *
     * @tparam T Тип возвращаемого значения; по умолчанию void.
     * @endif
     * @if EN
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
     * @endif
     */
    template <typename T = void>
    class [[nodiscard]] AsyncTask final {
    public:
        /**
         * @if RU
         * @brief Coroutine promise, связывающий frame, scope и shared state.
         *
         * State выделяется отдельно с MiAllocator. При неудаче аллокации
         * get_return_object возвращает ошибочную AsyncTask, а тело не исполняется.
         * @endif
         * @if EN
         * @brief Coroutine promise that connects the frame, scope, and shared state.
         *
         * State is allocated separately using MiAllocator. If allocation fails,
         * get_return_object returns an errored AsyncTask and the body is not run.
         * @endif
         */
        struct promise_type : AsyncTaskReturnOps<T, promise_type> {
            /// @if RU
            /// Не владеющий scope родителя; frame должен завершиться до scope.
            /// @endif
            /// @if EN
            /// Non-owning parent scope; the frame must complete before the scope.
            /// @endif
            CoroScope* m_coroScope{ nullptr };
            /// @if RU
            /// Producer-ссылка на shared state.
            /// @endif
            /// @if EN
            /// Producer reference to shared state.
            /// @endif
            AsyncPromise<T> m_promise;
            /// @if RU
            /// Consumer-ссылка, передаваемая наружу из coroutine.
            /// @endif
            /// @if EN
            /// Consumer reference returned from the coroutine.
            /// @endif
            AsyncFuture<T> m_future;
            /// @if RU
            /// Флаг отказа создания shared state.
            /// @endif
            /// @if EN
            /// Flag indicating failure to create shared state.
            /// @endif
            bool m_stateCreationFailed{ false };

            /// @if RU
            /// Инициализирует state для coroutine с первым объектом-параметром и scope.
            /// @tparam TObj Тип первого coroutine-параметра.
            /// @tparam TArgs Типы оставшихся coroutine-параметров.
            /// @param scope Scope, хранящийся в frame.
            /// @endif
            /// @if EN
            /// Initializes state for a coroutine whose first parameter is an object followed by a scope.
            /// @tparam TObj Type of the first coroutine parameter.
            /// @tparam TArgs Types of the remaining coroutine parameters.
            /// @param scope Scope stored in the frame.
            /// @endif
            template <typename TObj, typename... TArgs>
            promise_type(TObj&&, CoroScope& scope, TArgs&...) noexcept
                : m_coroScope(&scope) {
                initializeState(scope.canImmediateLaunched());
            }

            /// @if RU
            /// Инициализирует state для coroutine, начинающейся с CoroScope.
            /// @tparam TArgs Типы дополнительных coroutine-параметров.
            /// @param scope Scope, хранящийся в frame.
            /// @endif
            /// @if EN
            /// Initializes state for a coroutine whose first parameter is CoroScope.
            /// @tparam TArgs Types of the additional coroutine parameters.
            /// @param scope Scope stored in the frame.
            /// @endif
            template <typename... TArgs>
            promise_type(CoroScope& scope, TArgs&...) noexcept
                : m_coroScope(&scope) {
                initializeState(scope.canImmediateLaunched());
            }

            /// @if RU
            /// Создаёт promise без scope, применяемый для coroutine без scope-параметра.
            /// @endif
            /// @if EN
            /// Creates a promise without a scope, used for coroutines without a scope parameter.
            /// @endif
            promise_type() noexcept = default;

            /// @if RU
            /// Создаёт AsyncTask из состояния и регистрирует coroutine handle в scope.
            /// @return Ошибка allocation_failure, если state не удалось выделить.
            /// @endif
            /// @if EN
            /// Creates an AsyncTask from the state and registers the coroutine handle with the scope.
            /// @return allocation_failure if state allocation failed.
            /// @endif
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

            /// @if RU
            /// Coroutine-frame allocation failure становится ошибочной AsyncTask.
            /// @endif
            /// @if EN
            /// Coroutine-frame allocation failure produces an errored AsyncTask.
            /// @endif
            [[nodiscard]] static AsyncTask get_return_object_on_allocation_failure() noexcept {
                return AsyncTask{ Error::allocation_failure };
            }

            /// @if RU
            /// Выбирает начальный suspend на основании launch mode и результата аллокации.
            /// @endif
            /// @if EN
            /// Selects initial suspension based on the launch mode and allocation result.
            /// @endif
            [[nodiscard]] auto initial_suspend() const noexcept {
                return InitialAwaiter{m_coroScope, m_stateCreationFailed};
            }
            /// @if RU
            /// Создаёт финализатор, который сначала публикует завершение, затем уничтожает frame.
            /// @endif
            /// @if EN
            /// Creates a finalizer that publishes completion before destroying the frame.
            /// @endif
            [[nodiscard]] auto final_suspend() const noexcept {
                return FinalAwaiter{
                    m_coroScope,
                    m_promise.stateAddress(),
                    &AsyncPromise<T>::retainState,
                    &AsyncPromise<T>::releaseState,
                    &AsyncPromise<T>::completeState
                };
            }

            /// @if RU
            /// Исключения из пользовательского тела корутины фатальны.
            /// @endif
            /// @if EN
            /// Exceptions from the user coroutine body are fatal.
            /// @endif
            void unhandled_exception() const noexcept { std::terminate(); }

            /// @if RU
            /// Добавляет cancellation flag к поддерживающему это awaitable.
            /// @tparam Awaitable Тип одноразового awaitable с claimAwait/prepareAwaiter.
            /// @param awaitable Awaitable, чьё потребление передаётся promise.
            /// @return Подготовленный awaiter.
            /// Повторное потребление awaitable является фатальной ошибкой.
            /// @endif
            /// @if EN
            /// Adds the cancellation flag to a supported awaitable.
            /// @tparam Awaitable One-shot awaitable with claimAwait/prepareAwaiter.
            /// @param awaitable Awaitable whose consumption is transferred to the promise.
            /// @return Prepared awaiter.
            /// Reusing an awaitable is a fatal error.
            /// @endif
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

            /// @if RU
            /// Настраивает вложенную задачу на pinned executor текущего потока.
            /// @tparam U Тип результата вложенной задачи.
            /// @param task Вложенная задача, потребляемая единственным consumer-ом.
            /// @return Awaiter, распространяющий cancellation flag этой задачи.
            /// @endif
            /// @if EN
            /// Configures the nested task to use the current thread's pinned executor.
            /// @tparam U Result type of the nested task.
            /// @param task Nested task consumed by the sole consumer.
            /// @return Awaiter that propagates this task's cancellation flag.
            /// @endif
            template <typename U>
            [[nodiscard]] AsyncFuture<U>::FutureAwaiter await_transform(AsyncTask<U>& task) noexcept {
                return task.getAwaiter(
                    IExecutor::getThreadPinnedExecutor(), m_promise.cancellationFlag());
            }

            /// @if RU
            /// Настраивает вложенную задачу на pinned executor текущего потока.
            /// @tparam U Тип результата вложенной задачи.
            /// @param task Временная или перемещённая задача.
            /// @return Awaiter, распространяющий cancellation flag этой задачи.
            /// @endif
            /// @if EN
            /// Configures the nested task to use the current thread's pinned executor.
            /// @tparam U Result type of the nested task.
            /// @param task Temporary or moved task.
            /// @return Awaiter that propagates this task's cancellation flag.
            /// @endif
            template <typename U>
            [[nodiscard]] AsyncFuture<U>::FutureAwaiter await_transform(AsyncTask<U>&& task) noexcept {
                return task.getAwaiter(
                    IExecutor::getThreadPinnedExecutor(), m_promise.cancellationFlag());
            }

            /// @if RU
            /// Проверяет cancellation flag без приостановки.
            /// @endif
            /// @if EN
            /// Checks the cancellation flag without suspending.
            /// @endif
            struct CancellationAwaiter {
                /// @if RU
                /// Не владеющий флаг родительской задачи.
                /// @endif
                /// @if EN
                /// Non-owning flag of the parent task.
                /// @endif
                std::atomic<bool>* flag;
                /// @if RU
                /// Всегда готов: отмена только считывается.
                /// @endif
                /// @if EN
                /// Always ready: cancellation is only read.
                /// @endif
                [[nodiscard]] bool await_ready() const noexcept { return true; }
                /// @if RU
                /// Не используется, поскольку await_ready всегда true.
                /// @endif
                /// @if EN
                /// Unused because await_ready is always true.
                /// @endif
                void await_suspend(std::coroutine_handle<>) const noexcept {}
                /// @if RU
                /// Возвращает снимок флага отмены.
                /// @endif
                /// @if EN
                /// Returns a snapshot of the cancellation flag.
                /// @endif
                [[nodiscard]] AsyncResult<void> await_resume() const noexcept {
                    return AsyncResult<void>{.isCancelled = flag->load(std::memory_order_acquire)};
                }
            };

            /// @if RU
            /// Преобразует маркер cancellationPoint в CancellationAwaiter.
            /// @endif
            /// @if EN
            /// Converts a cancellationPoint marker to a CancellationAwaiter.
            /// @endif
            [[nodiscard]] CancellationAwaiter await_transform(CancellationPoint) noexcept {
                return CancellationAwaiter{m_promise.cancellationFlag()};
            }

            /// @if RU
            /// Передаёт остальные неподдерживаемые awaitable без преобразования.
            /// @tparam Awaitable Тип произвольного awaitable.
            /// @param awaitable Объект, передаваемый с сохранением категории значения.
            /// @return Исходный объект или ссылка на него.
            /// @endif
            /// @if EN
            /// Forwards other unsupported awaitables without transformation.
            /// @tparam Awaitable Type of the arbitrary awaitable.
            /// @param awaitable Object forwarded with its value category preserved.
            /// @return The original object or a reference to it.
            /// @endif
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
             * @if RU
             * @brief Awaiter переключения текущей coroutine на заданный executor.
             *
             * Всегда проходит через await_suspend. При уже выставленной отмене,
             * nullptr executor-е или ошибке execute() продолжает текущий handle
             * на текущем потоке; при успехе возвращает dispatcher текущего потока.
             * @endif
             * @if EN
             * @brief Awaiter that switches the current coroutine to a specified executor.
             *
             * Always goes through await_suspend. If cancellation is already set,
             * the executor is null, or execute() fails, it continues the current
             * handle on the current thread; on success, it returns the current
             * thread's dispatcher.
             * @endif
             */
            struct SwitchExecutorAwaiter final {
                /// @if RU
                /// Не владеющий целевой executor.
                /// @endif
                /// @if EN
                /// Non-owning target executor.
                /// @endif
                IExecutor* targetExecutor{ nullptr };
                /// @if RU
                /// Не владеющий флаг отмены родительской задачи.
                /// @endif
                /// @if EN
                /// Non-owning cancellation flag of the parent task.
                /// @endif
                std::atomic<bool>* parentCancelledFlag{ nullptr };

                /// @if RU
                /// Всегда false: попытка переключения выполняется в await_suspend.
                /// @endif
                /// @if EN
                /// Always false: the switch attempt is performed in await_suspend.
                /// @endif
                [[nodiscard]] constexpr bool await_ready() const noexcept {
                    // Мы ВСЕГДА приостанавливаемся, чтобы честно выполнить
                    // маршаллинг задачи в очередь нового исполнителя
                    return false;
                }

                /// @if RU
                /// Ставит coroutine handle в executor либо продолжает его немедленно.
                /// @param currentCoro Handle текущей корутины.
                /// @return Dispatcher handle при успешной постановке, иначе currentCoro.
                /// @endif
                /// @if EN
                /// Enqueues the coroutine handle on the executor or continues it immediately.
                /// @param currentCoro Handle of the current coroutine.
                /// @return Dispatcher handle on successful enqueue; otherwise, currentCoro.
                /// @endif
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

                // =================================================================
                // ТОЧКА ПРОБОРКИ ОТМЕНЫ ПОСЛЕ ПЕРЕКЛЮЧЕНИЯ (Ваша идея!)
                // =================================================================
                /// @if RU
                /// Возвращает состояние отмены после переключения/немедленного продолжения.
                /// @endif
                /// @if EN
                /// Returns the cancellation state after switching or immediate continuation.
                /// @endif
                [[nodiscard]] AsyncResult<void> await_resume() const noexcept {
                    bool const cancelled = parentCancelledFlag ? parentCancelledFlag->load(std::memory_order_acquire) : false;

                    // Возвращаем ультралегкий флаг. Если false — пользователь прервет выполнение.
                    return AsyncResult<void>{ .isCancelled = cancelled };
                }
            };

            /// @if RU
            /// Позволяет писать `co_await executor` для переключения исполнителя.
            /// @param explicitExecutor Не владеющий целевой executor либо nullptr.
            /// @return Awaiter переключения; nullptr означает немедленное продолжение.
            /// @endif
            /// @if EN
            /// Allows `co_await executor` to switch executors.
            /// @param explicitExecutor Non-owning target executor or nullptr.
            /// @return Switch awaiter; nullptr means immediate continuation.
            /// @endif
            [[nodiscard]] auto await_transform(IExecutor* explicitExecutor) noexcept {
                return SwitchExecutorAwaiter{
                    .targetExecutor = explicitExecutor,
                    .parentCancelledFlag = m_promise.cancellationFlag()
                };
            }

            /// @if RU
            /// Создаёт allocator-owned shared state; неуспех запоминается для return object.
            /// @param started Начальное состояние флага запуска.
            /// @endif
            /// @if EN
            /// Creates allocator-owned shared state; failure is recorded for the return object.
            /// @param started Initial state of the started flag.
            /// @endif
            void initializeState(bool started) noexcept {
                auto state = makeAsyncState<T>(MiAllocator<std::byte>{}, started);
                if (!state) [[unlikely]] {
                    m_stateCreationFailed = true;
                    return;
                }
                m_promise = std::move(state->promise);
                m_future = std::move(state->future);
            }

            /// @if RU
            /// Выделяет coroutine frame через MiAllocator.
            /// @endif
            /// @if EN
            /// Allocates the coroutine frame using MiAllocator.
            /// @endif
            [[nodiscard]] void* operator new(std::size_t size) noexcept {
                return MiAllocator<char>{}.allocate(size);
            }
            /// @if RU
            /// Освобождает coroutine frame, выделенный operator new.
            /// @endif
            /// @if EN
            /// Frees the coroutine frame allocated by operator new.
            /// @endif
            void operator delete(void* ptr, std::size_t) noexcept {
                MiAllocator<char>{}.deallocate(static_cast<char*>(ptr));
            }
        };

        /// @if RU
        /// Создаёт задачу из Future и coroutine frame; владение обоими передаётся объекту.
        /// @param future Consumer-ссылка shared state.
        /// @param handle Владеемый coroutine frame.
        /// @endif
        /// @if EN
        /// Creates a task from a Future and coroutine frame, taking ownership of both.
        /// @param future Consumer reference to shared state.
        /// @param handle Owned coroutine frame.
        /// @endif
        AsyncTask(AsyncFuture<T> future, std::coroutine_handle<promise_type> handle) noexcept
            : m_future{std::move(future)}, m_handle{handle} {}
        /// @if RU
        /// Создаёт пустую ошибочную задачу.
        /// @param error Причина, по которой задача недоступна.
        /// @endif
        /// @if EN
        /// Creates an empty task in an error state.
        /// @param error Reason the task is unavailable.
        /// @endif
        AsyncTask(Error error) noexcept
            : m_error{error} {}
        /// @if RU
        /// Создаёт ошибочную задачу, которая всё ещё владеет frame и разрушит его.
        /// @param error Причина ошибки.
        /// @param handle Frame, подлежащий уничтожению при разрушении объекта.
        /// @endif
        /// @if EN
        /// Creates an errored task that still owns the frame and will destroy it.
        /// @param error Error reason.
        /// @param handle Frame to destroy when the object is destroyed.
        /// @endif
        AsyncTask(Error error, std::coroutine_handle<promise_type> handle) noexcept
            : m_handle{handle}, m_error{error} {}

        /**
         * @if RU
         * @brief Выставляет отмену при разрушении пока принадлежащей задаче.
         *
         * Если задача содержит ошибку, frame уничтожается немедленно; иначе
         * устанавливается только shared cancellation flag. Исполняющийся frame
         * продолжает жить до собственной финализации; frame явно не уничтожается
         * этим деструктором.
         * @endif
         * @if EN
         * @brief Requests cancellation when the task is destroyed while it still owns it.
         *
         * If the task contains an error, the frame is destroyed immediately;
         * otherwise, only the shared cancellation flag is set. A running frame
         * remains alive until its own finalization; this destructor does not
         * explicitly destroy the frame.
         * @endif
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
         * @if RU
         * @brief Выставляет cooperative cancellation flag и передаёт задачу токену.
         * @return Токен для ожидания завершения; запрос отмены сам по себе не
         *         сигнализирует Future и не прерывает тело корутины.
         * @endif
         * @if EN
         * @brief Sets the cooperative cancellation flag and transfers the task to a token.
         * @return Token for waiting for completion; the cancellation request
         *         alone does not signal the Future or interrupt the coroutine body.
         * @endif
         */
        [[nodiscard]] CancellationToken<T> cancel() noexcept;

        /// @if RU
        /// @return Текущее состояние cancellation flag; пустая задача считается отменённой.
        /// @endif
        /// @if EN
        /// @return Current cancellation flag state; an empty task is considered cancelled.
        /// @endif
        [[nodiscard]] bool isCanceled() const noexcept {
            return m_future.isCanceled();
        }

        /// @if RU
        /// Копирование запрещено, поскольку задача уникально владеет coroutine handle.
        /// @endif
        /// @if EN
        /// Copying is disabled because the task uniquely owns its coroutine handle.
        /// @endif
        AsyncTask(const AsyncTask&) = delete;
        AsyncTask& operator=(const AsyncTask&) = delete;

        /// @if RU
        /// Передаёт владение handle и state; источник становится пустым.
        /// @param other Источник; после перемещения он не владеет frame/state.
        /// @endif
        /// @if EN
        /// Transfers ownership of the handle and state, leaving the source empty.
        /// @param other Source, which no longer owns the frame/state after the move.
        /// @endif
        AsyncTask(AsyncTask&& other) noexcept
            : m_future(std::move(other.m_future)),
              m_handle(std::exchange(other.m_handle, nullptr)),
              m_error{other.m_error} {
            other.m_error.reset();
        }

        /// @if RU
        /// Освобождает прежний Future и принимает владение источника.
        /// Прежний handle не уничтожается явно, а его cancellation flag не меняется.
        /// @param other Источник владения; самоприсваивание безопасно.
        /// @return Ссылка на этот AsyncTask.
        /// @endif
        /// @if EN
        /// Releases the previous Future and takes ownership from the source.
        /// The previous handle is not explicitly destroyed and its cancellation
        /// flag is unchanged.
        /// @param other Ownership source; self-assignment is safe.
        /// @return Reference to this AsyncTask.
        /// @endif
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
         * @if RU
         * @brief Запускает задачу при необходимости и готовит ожидание на executor-е.
         * @param explicitExecutor Не владеющий executor для возобновления waiter-а;
         *                         nullptr допустим только если результат уже готов.
         * @return Awaitable с собственной ссылкой на state.
         * @note Для старта используется pinned executor текущего TLS-контекста;
         *       explicitExecutor применяется для возврата ожидающего caller-а.
         * @note При отказе старта await-path может возобновить coroutine на текущем потоке.
         * @note Thread safety: не синхронизирует одновременное перемещение/ожидание одного объекта.
         * @note Передаваемый frame handle снимается с AsyncTask, чтобы деструктор
         *       не выставлял ложную отмену после передачи ожидателю.
         * @endif
         * @if EN
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
         * @endif
         */
        [[nodiscard]] AsyncFuture<T>::FutureAwaitable get(IExecutor* explicitExecutor) noexcept {
            requireValid();
            startForAwait();
            if (explicitExecutor == nullptr && !m_future.isSignaled()) [[unlikely]] {
                std::terminate();
            }
            // Мы бережно передаем хэндл в авейтер, обнуляя его у себя (std::exchange).
            // Наш деструктор ~AsyncTask() увидит nullptr и НЕ вызовет ложную отмену!
            std::exchange(m_handle, nullptr);
            return m_future.prepareAwaitable(explicitExecutor);
        }

        /**
         * @if RU
         * @brief Готовит ожидание с pinned executor текущего TLS-контекста.
         * @return Awaitable результата.
         * @note Для старта используется pinned executor; если постановка не удалась,
         *       startForAwait возобновляет coroutine непосредственно в текущем потоке.
         *       При отсутствии executor-а и незавершённой задаче вызывает terminate.
         * @note Thread safety: не синхронизирует одновременное использование этого объекта.
         * @endif
         * @if EN
         * @brief Prepares to await using the pinned executor from the current TLS context.
         * @return Awaitable for the result.
         * @note The pinned executor is used to start the task; if enqueueing fails,
         *       startForAwait resumes the coroutine directly on the current thread.
         *       If no executor is available and the task is incomplete, terminate is called.
         * @note Thread safety: does not synchronize concurrent use of this object.
         * @endif
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
         * @if RU
         * @brief Создаёт awaiter для вложенного co_await с флагом родителя.
         * @param executor Executor возобновления, не владеющий указатель.
         * @param cancellationFlag Флаг отмены родительской корутины либо nullptr.
         * @return Awaiter, который единожды потребляет результат.
         * @pre Задача валидна; повторный consumer вызывает terminate.
         * @note При отказе запуска coroutine может быть синхронно возобновлена
         *       на текущем потоке до возврата из этого метода.
         * @endif
         * @if EN
         * @brief Creates an awaiter for nested co_await with the parent's cancellation flag.
         * @param executor Non-owning executor used to resume the waiter.
         * @param cancellationFlag Parent coroutine's cancellation flag, or nullptr.
         * @return Awaiter that consumes the result exactly once.
         * @pre The task is valid; a repeated consumer calls terminate.
         * @note If startup fails, the coroutine may be resumed synchronously on
         *       the current thread before this method returns.
         * @endif
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

        /// @if RU
        /// Эквивалентен `co_await get()` с pinned executor текущего потока.
        /// @return FutureAwaitable для результата задачи.
        /// @endif
        /// @if EN
        /// Equivalent to `co_await get()` using the current thread's pinned executor.
        /// @return FutureAwaitable for the task result.
        /// @endif
        auto operator co_await() noexcept {
            return get(); // Просто перенаправляем в дефолтный TaskAwaiter
        }

        /**
         * @if RU
         * @brief Ставит задачу на указанный executor, если она ещё не запущена.
         * @param executor Executor для запуска; nullptr возвращает executor_unavailable.
         * @return Успех при уже сигнализированной/запущенной задаче; иначе ошибка
         *         allocation_failure, invalid_coroutine_handle, executor_unavailable
         *         или ошибка постановки executor-а.
         * @note Thread safety: операция старта дедуплицируется атомарным флагом shared state;
         *       владение AsyncTask и его coroutine handle не потокобезопасно.
         * @endif
         * @if EN
         * @brief Enqueues the task on the specified executor if it has not started yet.
         * @param executor Executor used to start the task; nullptr returns executor_unavailable.
         * @return Success if already signaled/started; otherwise allocation_failure,
         *         invalid_coroutine_handle, executor_unavailable, or an executor enqueue error.
         * @note Thread safety: start is deduplicated atomically in shared state;
         *       ownership of AsyncTask and its coroutine handle is not thread-safe.
         * @endif
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

        /// @if RU
        /// Запускает задачу на pinned executor текущего потока.
        /// @return Результат start(IExecutor*); без TLS executor-а возвращает ошибку,
        ///         если задача ещё не завершена.
        /// @endif
        /// @if EN
        /// Starts the task on the current thread's pinned executor.
        /// @return Result of start(IExecutor*); without a TLS executor, returns an
        ///         error if the task has not completed.
        /// @endif
        Status start() noexcept {
            return start(IExecutor::getThreadPinnedExecutor());
        }

        /// @if RU
        /// @return true, если указатель handle ненулевой и ошибка отсутствует;
        ///         не гарантирует, что frame ещё не завершился.
        /// @endif
        /// @if EN
        /// @return true if the handle is non-null and no error is present;
        ///         does not guarantee that the frame has not completed.
        /// @endif
        [[nodiscard]] bool has_value() const noexcept { return m_handle != nullptr && !m_error.has_value(); }
        /// @if RU
        /// Возвращает не владеющий handle; требует has_value() и незавершённого frame.
        /// @return Handle принадлежащего задаче frame.
        /// @warning После немедленного завершения handle может уже не быть валидным.
        /// @endif
        /// @if EN
        /// Returns a non-owning handle; requires has_value() and an incomplete frame.
        /// @return Handle to the frame owned by the task.
        /// @warning After immediate completion, the handle may no longer be valid.
        /// @endif
        std::coroutine_handle<> get_handle() noexcept {
            assert(has_value());
            return m_handle;
        }
        /// @if RU
        /// Возвращает ошибку создания/постановки, если она присутствует.
        /// @endif
        /// @if EN
        /// Returns the creation/enqueue error, if any.
        /// @endif
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

        /// @if RU
        /// Future-ссылка на общий результат.
        /// @endif
        /// @if EN
        /// Future reference to the shared result.
        /// @endif
        AsyncFuture<T> m_future{};
        /// @if RU
        /// Владеющий handle frame, очищаемый при завершении или отмене объекта.
        /// @endif
        /// @if EN
        /// Owning frame handle, cleared when the task completes or is cancelled.
        /// @endif
        std::coroutine_handle<promise_type> m_handle{ nullptr };
        /// @if RU
        /// Ошибка создания либо старта; отсутствие ошибки не гарантирует завершение.
        /// @endif
        /// @if EN
        /// Creation or startup error; absence of an error does not imply completion.
        /// @endif
        std::optional<Error> m_error;
    };

    /**
     * @if RU
     * @brief Move-only представление отменённой задачи.
     *
     * Владеет перемещённой AsyncTask и единожды ожидает её результат. Запрос
     * отмены кооперативный; awaiter этого токена принудительно выставляет
     * isCancelled в AsyncResult, независимо от значения producer-а.
     *
     * @tparam T Тип результата обёрнутой задачи.
     * @endif
     * @if EN
     * @brief Move-only representation of a cancelled task.
     *
     * Owns a moved AsyncTask and awaits its result exactly once. Cancellation is
     * cooperative; this token's awaiter forcibly sets isCancelled in AsyncResult,
     * regardless of the producer's value.
     *
     * @tparam T Result type of the wrapped task.
     * @endif
     */
    template <typename T>
    class [[nodiscard]] CancellationToken final {
    public:
        /// @if RU
        /// Принимает во владение задачу и её coroutine handle.
        /// @endif
        /// @if EN
        /// Takes ownership of the task and its coroutine handle.
        /// @endif
        explicit CancellationToken(AsyncTask<T>&& task) noexcept
            : m_task{std::move(task)} {}

        /// @if RU
        /// Копирование запрещено: токен владеет одноразовой задачей.
        /// @endif
        /// @if EN
        /// Copying is disabled because the token owns a one-shot task.
        /// @endif
        CancellationToken(const CancellationToken&) = delete;
        CancellationToken& operator=(const CancellationToken&) = delete;
        /// @if RU
        /// Передаёт владение задачей.
        /// @endif
        /// @if EN
        /// Transfers ownership of the task.
        /// @endif
        CancellationToken(CancellationToken&&) noexcept = default;
        /// @if RU
        /// Передаёт владение, освобождая ранее удерживаемую задачу.
        /// @endif
        /// @if EN
        /// Transfers ownership, releasing the previously held task.
        /// @endif
        CancellationToken& operator=(CancellationToken&&) noexcept = default;

        /// @if RU
        /// @return true, если обёрнутая задача содержит действительный handle.
        /// @endif
        /// @if EN
        /// @return true if the wrapped task contains a valid handle.
        /// @endif
        [[nodiscard]] bool has_value() const noexcept {
            return m_task.has_value();
        }

        /// @if RU
        /// @return Ошибка создания или запуска задачи, если она есть.
        /// @endif
        /// @if EN
        /// @return Task creation or startup error, if any.
        /// @endif
        [[nodiscard]] std::optional<Error> getError() const noexcept {
            return m_task.getError();
        }

        /// @if RU
        /// Awaiter, делегирующий планирование FutureAwaiter и помечающий результат отменённым.
        /// @endif
        /// @if EN
        /// Awaiter that delegates scheduling to FutureAwaiter and marks the result cancelled.
        /// @endif
        struct Awaiter {
            /// @if RU
            /// Внутренний awaiter, удерживающий ссылку на state.
            /// @endif
            /// @if EN
            /// Inner awaiter that retains a reference to the state.
            /// @endif
            AsyncFuture<T>::FutureAwaiter m_taskAwaiter;

            /// @if RU
            /// @return true, если задача уже сигнализирована.
            /// @endif
            /// @if EN
            /// @return true if the task has already been signaled.
            /// @endif
            [[nodiscard]] bool await_ready() const noexcept {
                return m_taskAwaiter.await_ready();
            }

            /// @if RU
            /// Делегирует регистрацию waiter-а и планирование общему состоянию.
            /// @endif
            /// @if EN
            /// Delegates waiter registration and scheduling to the shared state.
            /// @endif
            std::coroutine_handle<> await_suspend(std::coroutine_handle<> handle) noexcept {
                return m_taskAwaiter.await_suspend(handle);
            }

            /// @if RU
            /// Возвращает результат с принудительно установленным флагом отмены.
            /// @endif
            /// @if EN
            /// Returns the result with the cancellation flag forcibly set.
            /// @endif
            [[nodiscard]] auto await_resume() noexcept {
                auto result = m_taskAwaiter.await_resume();
                result.isCancelled = true;
                return result;
            }
        };

        /**
         * @if RU
         * @brief Подготавливает одноразовый awaiter обёрнутой задачи.
         * @param resumeExecutor Не владеющий executor для возобновления waiter-а.
         * @return Awaiter; nullptr допустим только при уже опубликованном завершении.
         * @endif
         * @if EN
         * @brief Prepares a one-shot awaiter for the wrapped task.
         * @param resumeExecutor Non-owning executor used to resume the waiter.
         * @return Awaiter; nullptr is allowed only if completion has already been published.
         * @endif
         */
        [[nodiscard]] Awaiter get(IExecutor* resumeExecutor = IExecutor::getThreadPinnedExecutor()) noexcept {
            return Awaiter{m_task.getAwaiter(resumeExecutor, nullptr)};
        }

        /// @if RU
        /// Ожидает, используя pinned executor текущего потока.
        /// @endif
        /// @if EN
        /// Awaits using the current thread's pinned executor.
        /// @endif
        Awaiter operator co_await() & noexcept {
            return get();
        }

        /// @if RU
        /// Использует тот же путь ожидания, что и lvalue-перегрузка.
        /// @endif
        /// @if EN
        /// Uses the same waiting path as the lvalue overload.
        /// @endif
        Awaiter operator co_await() && noexcept {
            return operator co_await();
        }

    private:
        /// @if RU
        /// Единственная задача, ожидаемая синхронным get().
        /// @endif
        /// @if EN
        /// Sole task awaited by synchronous get().
        /// @endif
        AsyncTask<T> m_task;
    };

    /**
     * @if RU
     * @brief Запрашивает кооперативную отмену и передаёт задачу токену.
     * @return Move-only токен для ожидания завершения задачи.
     * @endif
     * @if EN
     * @brief Requests cooperative cancellation and transfers the task to a token.
     * @return Move-only token for awaiting task completion.
     * @endif
     */
    template <typename T>
    CancellationToken<T> AsyncTask<T>::cancel() noexcept {
        m_future.cancel();
        return CancellationToken<T>{std::move(*this)};
    }

    /**
     * @if RU
     * @brief Обёртка AsyncTask, get() которой блокирует вызывающий OS-поток.
     *
     * Используется перегрузкой JoinableScope::launch с явными options. start()
     * остаётся асинхронным; get() устанавливает BlockingExecutor в TLS текущего
     * потока и ждёт сигнализации shared state. Для non-void T get() возвращает
     * ссылку rvalue на значение внутри состояния, а не владеющее значение.
     * Отмену и наличие payload перед извлечением этот интерфейс не проверяет.
     *
     * @tparam T Тип результата обёрнутой задачи.
     * @endif
     * @if EN
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
     * @endif
     */
    template <typename T>
    class BlockingTask {
        /// @if RU
        /// Executor сигнализации, lifetime которого покрывает синхронный get().
        /// @endif
        /// @if EN
        /// Signaling executor whose lifetime covers synchronous get().
        /// @endif
        BlockingExecutor m_executor{};
    public:
        /// @if RU
        /// Принимает во владение AsyncTask.
        /// @endif
        /// @if EN
        /// Takes ownership of an AsyncTask.
        /// @endif
        BlockingTask(AsyncTask<T>&& task) noexcept : m_task{std::move(task)} {}

        /// @if RU
        /// Запускает задачу на переданном executor-е.
        /// @param executor Не владеющий executor.
        /// @return Статус запуска задачи.
        /// @endif
        /// @if EN
        /// Starts the task on the supplied executor.
        /// @param executor Non-owning executor.
        /// @return Task startup status.
        /// @endif
        Status start(IExecutor* executor) noexcept {
            return m_task.start(executor);
        }
        /// @if RU
        /// Запускает на pinned executor текущего потока.
        /// @return Статус запуска задачи.
        /// @endif
        /// @if EN
        /// Starts the task on the current thread's pinned executor.
        /// @return Task startup status.
        /// @endif
        Status start() noexcept {
            return m_task.start();
        }

        /// @if RU
        /// Запрашивает отмену и передаёт владение задачей токену.
        /// @endif
        /// @if EN
        /// Requests cancellation and transfers ownership of the task to a token.
        /// @endif
        [[nodiscard]] CancellationToken<T> cancel() noexcept {
            return m_task.cancel();
        }

        /// @if RU
        /// @return true, если coroutine handle обёрнутой задачи валиден.
        /// @endif
        /// @if EN
        /// @return true if the wrapped task's coroutine handle is valid.
        /// @endif
        [[nodiscard]] bool has_value() const noexcept {
            return m_task.has_value();
        }

        /// @if RU
        /// @return Записанная ошибка задачи, если она есть.
        /// @endif
        /// @if EN
        /// @return The task's recorded error, if any.
        /// @endif
        [[nodiscard]] std::optional<Error> getError() const noexcept {
            return m_task.getError();
        }

        /**
         * @if RU
         * @brief Блокирует текущий OS-поток до сигнализации shared state.
         * @return void для T == void, иначе ссылка rvalue на сохранённый T.
         * @pre Задача валидна, consumer доступен; при асинхронном завершении
         *      должен быть настроен executor.
         * @warning Для non-void задач ссылка не владеющая и становится недействительной
         *          при уничтожении shared state. У отменённой задачи без значения
         *          извлекать результат нельзя.
         * @endif
         * @if EN
         * @brief Blocks the current OS thread until the shared state is signaled.
         * @return void for T == void; otherwise, an rvalue reference to the stored T.
         * @pre The task is valid and a consumer is available; an executor must
         *      be configured for asynchronous completion.
         * @warning For non-void tasks, the reference is non-owning and becomes
         *          invalid when the shared state is destroyed. Do not extract
         *          a result from a cancelled task with no value.
         * @endif
         */
        decltype(auto) get() {
            m_task.requireValid();
            m_task.requireSingleConsumer();
            m_task.startForAwait();
            auto threadContextGuard = m_executor.initializeThreadContext();
            // 1. Получаем awaitable, передавая адрес нашего встроенного m_executor.
            // Напрямую лезем во внутренний стейт AsyncTask.
            auto awaitable = m_task.m_future.prepareAwaitable(&m_executor);
            auto* state = awaitable.m_state;

            // -----------------------------------------------------------------
            // FAST-PATH ЭТАП 1: Данные УЖЕ готовы (Продюсер успел до вызова get)
            // -----------------------------------------------------------------
            if (state->isSignaled()) {
                if constexpr (std::same_as<T, void>) return;
                else return state->extractValue();
            }

            // -----------------------------------------------------------------
            // FAST-PATH ЭТАП 2: Гонка с продюсером в момент регистрации.
            // Если tryRegisterWaiter вернул false, значит пока мы записывали
            // указатели, продюсер СРАЗУ переключил стейт в Signaled!
            // -----------------------------------------------------------------
            if (!state->tryRegisterWaiter(std::noop_coroutine(), &m_executor)) {
                if constexpr (std::same_as<T, void>) return;
                else return state->extractValue();
            }

            // -----------------------------------------------------------------
            // SLOW-PATH ЭТАП 3: Данных нет, ждун успешно зарегистрирован.
            // Уводим физический поток ОС в жесткий анабиоз ядра на фьютексе/WaitOnAddress.
            // Защита от ложных пробуждений (while-цикл) уже внутри waitForSignal!
            // -----------------------------------------------------------------
            m_executor.waitForSignal();

            // Поток проснулся, данные гарантированно на месте.
            // Извлекаем r-value ссылку без единого копирования!
            if constexpr (std::same_as<T, void>) return;
            else return state->extractValue();
        }

    private:
        /// @if RU
        /// Единственная задача, ожидаемая синхронным get().
        /// @endif
        /// @if EN
        /// Sole task awaited by synchronous get().
        /// @endif
        AsyncTask<T> m_task;
    };

} // namespace shuttle::async
