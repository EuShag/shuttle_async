//
// Created by Shagu on 12.10.2026.
//

#ifndef SHUTTLEENGINE_COROSCOPE_HPP
#define SHUTTLEENGINE_COROSCOPE_HPP
#include <atomic>
#include <cinttypes>
#include <concepts>
#include <coroutine>
#include <future>
#include <functional>
#include <type_traits>
#include <utility>
#include <thread>

#include <async/i_executor.hpp>
#include <async/error.hpp>
#include <async/launch_mode.hpp>
#include <async/mi_allocator.hpp>

namespace shuttle::async {

    template <typename T>
    class AsyncTask;

    template <typename T>
    class BlockingTask;

    class CoroScope;

    /// @if RU
    /// Базовый trait: произвольный тип не считается AsyncTask.
    /// @endif
    /// @if EN
    /// Base trait: an arbitrary type is not considered an AsyncTask.
    /// @endif
    template <typename>
    struct IsAsyncTask : std::false_type {};

    /// @if RU
    /// Определяет AsyncTask как допустимый результат фабрики корутины.
    /// @endif
    /// @if EN
    /// Identifies AsyncTask as an allowed coroutine-factory result.
    /// @endif
    template <typename T>
    struct IsAsyncTask<AsyncTask<T>> : std::true_type {};

    /**
     * @if RU
     * @brief Концепт фабрики корутинного вызова CoroScope::launch.
     * @tparam F Фабрика, вызываемая с CoroScope и аргументами Args.
     * @tparam Args Типы дополнительных аргументов.
     * @return Ограничивает вызов, если результат фабрики — AsyncTask.
     * @endif
     * @if EN
     * @brief Factory concept for a CoroScope::launch coroutine invocation.
     * @tparam F Factory callable with a CoroScope and arguments Args.
     * @tparam Args Types of the additional arguments.
     * @return Constrains the call when the factory result is an AsyncTask.
     * @endif
     */
    template <typename F, typename... Args>
    concept CoroTaskFactory =
        std::invocable<F, CoroScope, Args...>
        && IsAsyncTask<std::remove_cvref_t<std::invoke_result_t<F, CoroScope, Args...>>>::value;

    /// @if RU
    /// Старший бит child counter сигнализирует, что scope больше не принимает детей.
    /// @endif
    /// @if EN
    /// The high bit of the child counter signals that the scope no longer accepts children.
    /// @endif
    inline constexpr uint64_t DEATH_SIGNAL = 1ULL << 63;

    /**
     * @if RU
     * @brief Исполнитель, который уведомляет один блокирующий поток.
     *
     * Используется JoinableScope::joinAll; execute не возобновляет переданный
     * handle напрямую, а устанавливает сигнал для waitForSignal.
     * @endif
     * @if EN
     * @brief Executor that notifies one blocked thread.
     *
     * Used by JoinableScope::joinAll; execute does not resume the supplied
     * handle directly, but sets a signal for waitForSignal.
     * @endif
     */
    class BlockingExecutor final : public IExecutor {
    public:
        /**
         * @if RU
         * @brief Сигнализирует ожидающему потоку.
         * @note Аргумент handle намеренно игнорируется.
         * @return Успешный Status.
         * @endif
         * @if EN
         * @brief Signals the waiting thread.
         * @note The handle argument is intentionally ignored.
         * @return Successful Status.
         * @endif
         */
        Status execute(std::coroutine_handle<>) override {
            // Жесткий release: публикуем все данные корутины для ждущего потока
            m_ready.store(true, std::memory_order_release);
            m_ready.notify_all();
            return {};
        }

        /// @if RU
        /// Блокируется до сигнала, затем сбрасывает его для следующего ожидания.
        /// @endif
        /// @if EN
        /// Blocks until signaled, then resets the signal for the next wait.
        /// @endif
        void waitForSignal() noexcept {
            // Защита от ложных пробуждений (Spurious Wakeups)
            // Если проснулись случайно, а флаг все еще false — уходим спать дальше
            while (!m_ready.load(std::memory_order_acquire)) {
                // Передаем false: засыпаем ТОЛЬКО если m_ready равен false.
                // Если он уже true, wait() мгновенно вернет управление без сна.
                m_ready.wait(false, std::memory_order_acquire);
            }

            // Сбрасываем в false для следующих итераций (например, для joinTask в деструкторе)
            m_ready.store(false, std::memory_order_relaxed);
        }

        /// @if RU
        /// Устанавливает TLS-контекст с этим executor-ом и noop dispatcher.
        /// @endif
        /// @if EN
        /// Sets a TLS context with this executor and a noop dispatcher.
        /// @endif
        ThreadContextGuard initializeThreadContext() noexcept {
            return ThreadContextGuard{{
                .m_sharedExecutor = this,
                .m_threadPinnedExecutor = this,
                .m_dispatcherCoroHandle = std::noop_coroutine()
            }};
        }

    private:
        /// @if RU
        /// Сигнал остаётся выставленным до возврата из waitForSignal().
        /// @endif
        /// @if EN
        /// Signal remains set until waitForSignal() returns.
        /// @endif
        std::atomic<bool> m_ready{false};
    };

    /**
     * @if RU
     * @brief Внутренняя coroutine, присоединяющаяся к CoroScope и ожидающая детей.
     *
     * Её frame регистрируется в scope, а initial awaiter проверяет, можно ли
     * немедленно завершить ожидание или требуется передать управление dispatcher-у.
     * @endif
     * @if EN
     * @brief Internal coroutine that joins a CoroScope and waits for its children.
     *
     * Its frame is registered with the scope, and the initial awaiter checks
     * whether the wait can complete immediately or must transfer control to a dispatcher.
     * @endif
     */
    class JointerTask {
    public:
        /// @if RU
        /// Начальный awaiter join coroutine.
        /// @endif
        /// @if EN
        /// Initial awaiter for the join coroutine.
        /// @endif
        struct Initial{
            /// @if RU
            /// Всегда приостанавливается для регистрации join handle.
            /// @endif
            /// @if EN
            /// Always suspends to register the join handle.
            /// @endif
            [[nodiscard]] bool await_ready() const noexcept { return false; }
            /// @if RU
            /// Регистрирует join coroutine в scope и выбирает transfer handle.
            /// @endif
            /// @if EN
            /// Registers the join coroutine with the scope and selects the transfer handle.
            /// @endif
            [[nodiscard]] std::coroutine_handle<> await_suspend(std::coroutine_handle<> coroHandle) const noexcept;
            /// @if RU
            /// Завершает начальное ожидание.
            /// @endif
            /// @if EN
            /// Completes the initial wait.
            /// @endif
            void await_resume() const noexcept {}
            /// @if RU
            /// Не владеющий scope, на котором выполняется join.
            /// Scope, чей join coroutine ожидает завершения потомков.
            /// @endif
            /// @if EN
            /// Non-owning scope on which the join is performed.
            /// Scope whose join coroutine waits for its descendants to complete.
            /// @endif
            CoroScope* m_scopePtr;
        };

        /// @if RU
        /// Coroutine promise для joinTask.
        /// @endif
        /// @if EN
        /// Coroutine promise for joinTask.
        /// @endif
        struct promise_type {
            /// @if RU
            /// Возвращает пустой маркер результата join coroutine.
            /// @endif
            /// @if EN
            /// Returns an empty result marker for the join coroutine.
            /// @endif
            auto get_return_object() noexcept {
                return JointerTask{};
            }

            /// @if RU
            /// Связывает promise с scope.
            /// @endif
            /// @if EN
            /// Associates the promise with the scope.
            /// @endif
            promise_type(CoroScope& scope) {
                m_scopePtr = &scope;
            }

            /// @if RU
            /// Создаёт awaiter, который публикует join handle.
            /// @endif
            /// @if EN
            /// Creates an awaiter that publishes the join handle.
            /// @endif
            [[nodiscard]] Initial initial_suspend() const noexcept { return Initial{m_scopePtr};}
            /// @if RU
            /// Завершает frame без дополнительной приостановки.
            /// @endif
            /// @if EN
            /// Completes the frame without additional suspension.
            /// @endif
            [[nodiscard]] std::suspend_never final_suspend() const noexcept { return {};}
            /// @if RU
            /// Необработанное исключение фатально.
            /// @endif
            /// @if EN
            /// An unhandled exception is fatal.
            /// @endif
            void unhandled_exception() noexcept { std::terminate(); }

            /// @if RU
            /// Обрабатывает штатное завершение join coroutine.
            /// @endif
            /// @if EN
            /// Handles normal completion of the join coroutine.
            /// @endif
            void return_void() const noexcept {}

            /// @if RU
            /// Выделяет frame через MiAllocator; отказ возвращает nullptr.
            /// @endif
            /// @if EN
            /// Allocates the frame using MiAllocator; returns nullptr on failure.
            /// @endif
            void* operator new(std::size_t size) noexcept {
                return MiAllocator<char>{}.allocate(size);
            }
            /// @if RU
            /// Освобождает frame через MiAllocator.
            /// @endif
            /// @if EN
            /// Frees the frame using MiAllocator.
            /// @endif
            void operator delete(void* ptr) noexcept {
                return MiAllocator<char>{}.deallocate(static_cast<char *>(ptr));
            }

            CoroScope* m_scopePtr;
        };
    };

    /**
     * @if RU
     * @brief Move-only lifetime token для дерева дочерних корутин.
     *
     * Scope передаётся по значению в AsyncTask-корутину и хранится в coroutine
     * frame. Создание child увеличивает счётчик родителя, а разрушение child
     * frame уменьшает его. Родительский объект обязан оставаться живым, пока
     * все его дочерние frame не завершились. LaunchOptions наследуются в
     * перегрузках launch без явных параметров.
     *
     * Immediate запускает тело при создании (до возврата фабрики), Async ставит
     * задачу в явный executor, Lazy возвращает ещё не запущенную задачу.
     * Ошибки создания/постановки возвращаются в AsyncTask. Внутренняя синхронизация
     * использует атомики; копирование запрещено, перемещение scope допустимо
     * только в рамках передачи его владения в coroutine frame.
     * @endif
     * @if EN
     * @brief Move-only lifetime token for a tree of child coroutines.
     *
     * A scope is passed by value into an AsyncTask coroutine and stored in its
     * frame. Creating a child increments the parent's counter; destroying the
     * child frame decrements it. The parent object must remain alive until all
     * child frames complete. LaunchOptions are inherited by launch overloads
     * without explicit options.
     *
     * Immediate starts the body during creation (before the factory returns),
     * Async enqueues the task on an explicit executor, and Lazy returns a task
     * that has not yet started. Creation/enqueue errors are returned in
     * AsyncTask. Internal synchronization uses atomics; copying is disabled, and
     * moving a scope is allowed only when transferring its ownership into a
     * coroutine frame.
     * @endif
     */
    class CoroScope {
    public:

        /// @if RU
        /// Уменьшает счётчик родителя при уничтожении дочернего scope.
        /// @endif
        /// @if EN
        /// Decrements the parent's counter when a child scope is destroyed.
        /// @endif
        ~CoroScope() {
            if (m_parentScope) {
                decrementChildCount();
            }
        };

        /// @if RU
        /// Scope нельзя копировать: он представляет уникальный child lifetime.
        /// @endif
        /// @if EN
        /// Scope is non-copyable because it represents a unique child lifetime.
        /// @endif
        CoroScope(const CoroScope&) = delete;
        CoroScope& operator=(const CoroScope&) = delete;
        /**
         * @if RU
         * @brief Передаёт scope в coroutine frame.
         *
         * Обнуляет родительскую ссылку и счётчики источника, чтобы временный
         * объект не декрементировал родителя при собственном разрушении.
         * @endif
         * @if EN
         * @brief Transfers a scope into a coroutine frame.
         *
         * Clears the source's parent reference and counters so the temporary
         * object does not decrement the parent when it is destroyed.
         * @endif
         */
        CoroScope(CoroScope&& other) noexcept
            : m_launch_options(other.m_launch_options),
              m_childCount(other.m_childCount.load(std::memory_order_relaxed)),
              m_threadExecutor(other.m_threadExecutor),
              m_ownerHandle(other.m_ownerHandle.load(std::memory_order_relaxed)),
              m_jointerTaskHandle(other.m_jointerTaskHandle.load(std::memory_order_relaxed)),
              m_parentScope(other.m_parentScope)
        {
            // Очищаем старый временный объект на стеке,
            // чтобы его деструктор НЕ сделал decrementChildCount() родителя!
            other.m_parentScope = nullptr;
            other.m_childCount.store(0, std::memory_order_relaxed);
            other.m_ownerHandle.store(nullptr, std::memory_order_relaxed);
        }

        /// @if RU
        /// Move assignment запрещён: scope передаётся конструированием в frame.
        /// @endif
        /// @if EN
        /// Move assignment is disabled; the scope is transferred into a frame by construction.
        /// @endif
        CoroScope& operator=(CoroScope&& other) = delete;

        /**
         * @if RU
         * @brief Создаёт child scope с указанными режимом и аргументами.
         * @tparam F Фабрика AsyncTask, принимающая дочерний CoroScope.
         * @tparam Args Типы аргументов фабрики.
         * @param func Фабрика задачи.
         * @param launchOptions Режим запуска и executor.
         * @param args Аргументы, передаваемые фабрике.
         * @return Созданная AsyncTask; ошибки фабрики/постановки остаются в ней.
         * @note В Async режиме задача ставится в executor до возврата; при nullptr
         *       или отказе постановки frame уничтожается и возвращается ошибка.
         *       Immediate/Lazy не проходят этот путь постановки.
         * @endif
         * @if EN
         * @brief Creates a child scope with the specified launch mode and arguments.
         * @tparam F AsyncTask factory accepting a child CoroScope.
         * @tparam Args Types of the factory arguments.
         * @param func Task factory.
         * @param launchOptions Launch mode and executor.
         * @param args Arguments forwarded to the factory.
         * @return Created AsyncTask; factory/enqueue errors remain in the task.
         * @note In Async mode the task is enqueued before return; if the executor
         *       is null or enqueueing fails, the frame is destroyed and an error
         *       is returned. Immediate/Lazy do not use this enqueue path.
         * @endif
         */
        template<typename F, typename... Args>
            requires CoroTaskFactory<F, Args...>
        auto launch(F&& func, LaunchOptions launchOptions, Args&&... args) noexcept {
            auto coro = std::invoke(
                std::forward<F>(func),
                this->createChild(launchOptions),
                std::forward<Args>(args)...
            );
            if (!coro.has_value()) [[unlikely]] {
                return coro;
            }

            if (launchOptions.mode == LaunchMode::Async) {
                auto childHandle = coro.get_handle();
                if (launchOptions.explicitExecutor == nullptr) [[unlikely]] {
                    childHandle.destroy();
                    return decltype(coro){Error::executor_unavailable};
                }

                auto queueResult = coro.start(launchOptions.explicitExecutor);
                if (!queueResult) [[unlikely]] {
                    childHandle.destroy();
                    return decltype(coro){queueResult.error()};
                }
            }

            return coro;
        }

        /**
         * @if RU
         * @brief Создаёт задачу, используя LaunchOptions текущего scope.
         * @tparam F Фабрика AsyncTask без дополнительных аргументов.
         * @param func Фабрика задачи.
         * @return Созданная AsyncTask или объект с ошибкой.
         * @endif
         * @if EN
         * @brief Creates a task using this scope's LaunchOptions.
         * @tparam F AsyncTask factory with no additional arguments.
         * @param func Task factory.
         * @return Created AsyncTask or an error object.
         * @endif
         */
        template<typename F>
            requires CoroTaskFactory<F>
        auto launch(F&& func) noexcept {
            return launch(std::forward<F>(func), m_launch_options);
        }

        /**
         * @if RU
         * @brief Создаёт задачу с аргументами и LaunchOptions текущего scope.
         * @tparam F Фабрика задачи.
         * @tparam FirstArg, Args Типы дополнительных параметров фабрики.
         * @param func Фабрика задачи.
         * @param firstArg Первый аргумент фабрики.
         * @param args Остальные аргументы фабрики.
         * @return Созданная AsyncTask или ошибка.
         * @endif
         * @if EN
         * @brief Creates a task with arguments and this scope's LaunchOptions.
         * @tparam F Task factory.
         * @tparam FirstArg, Args Types of the factory's additional parameters.
         * @param func Task factory.
         * @param firstArg First factory argument.
         * @param args Remaining factory arguments.
         * @return Created AsyncTask or an error.
         * @endif
         */
        template<typename F, typename FirstArg, typename... Args>
            requires (!std::same_as<std::remove_cvref_t<FirstArg>, LaunchOptions>)
                && CoroTaskFactory<F, FirstArg, Args...>
        auto launch(F&& func, FirstArg&& firstArg, Args&&... args) noexcept {
            return launch(
                std::forward<F>(func),
                m_launch_options,
                std::forward<FirstArg>(firstArg),
                std::forward<Args>(args)...
            );
        }

        /**
         * @if RU
         * @brief Создаёт child с Immediate режимом; выполнение начинается в текущем потоке.
         *
         * Выполнение корутины начинается синхронно в вызывающем контексте до первой точки приостановки.
         *
         * @warning **КРИТИЧЕСКОЕ ПРЕДУПРЕЖДЕНИЕ ПО БЕЗОПАСНОСТИ:**
         * Использовать оператор `co_await` внутри этой задачи разрешается **строго в рамках одного и того же потока**
         * (например, совместно с ThreadPinnedExecutor / OneThreadExecutor). Ни в коем случае не допускайте миграции
         * контекста выполнения на другие потоки или пулы потоков внутри Immediate-задач. Смена потока возобновления
         * нарушит иерархию Structured Concurrency и **приведёт к неопределённому поведению (UB)**.
         *
         * @tparam F Фабрика задачи; @tparam Args Типы аргументов фабрики.
         * @param func Фабрика задачи.
         * @param args Аргументы фабрики.
         * @return AsyncTask результата либо ошибка создания.
         * @endif
         *
         * @if EN
         * @brief Creates an Immediate-mode child; execution begins on the current thread.
         *
         * Coroutine execution starts synchronously in the calling context until the first suspension point.
         *
         * @warning **CRITICAL SAFETY WARNING:**
         * Using the `co_await` operator inside this task is allowed **strictly within the same thread context**
         * (e.g., in conjunction with a ThreadPinnedExecutor / OneThreadExecutor). Thread migration to other threads
         * or thread pools inside an Immediate task is strictly prohibited. Changing the execution context to a
         * different thread will break Structured Concurrency invariants and **lead to undefined behavior (UB)**.
         *
         * @tparam F Task factory; @tparam Args Factory argument types.
         * @param func Task factory.
         * @param args Factory arguments.
         * @return AsyncTask with a result or a creation error.
         * @endif
         */
        template<typename F, typename... Args>
            requires CoroTaskFactory<F, Args...>
        [[nodiscard]] auto launchImmediate(F&& func, Args&&... args) noexcept {
            return launch(std::forward<F>(func), {.mode = LaunchMode::Immediate, .explicitExecutor = nullptr}, std::forward<Args>(args)...);
        }

        /**
         * @if RU
         * @brief Создаёт child с Async режимом на указанном executor-е.
         * @tparam F Фабрика задачи без дополнительных аргументов.
         * @param func Фабрика задачи.
         * @param executor Не владеющий executor; по умолчанию pinned executor TLS.
         * @return Уже поставленная AsyncTask либо ошибка executor-а/очереди.
         * @endif
         * @if EN
         * @brief Creates an Async-mode child on the specified executor.
         * @tparam F Task factory with no additional arguments.
         * @param func Task factory.
         * @param executor Non-owning executor; defaults to the TLS pinned executor.
         * @return Already-enqueued AsyncTask or an executor/queue error.
         * @endif
         */
        template<typename F>
            requires CoroTaskFactory<F>
        [[nodiscard]] auto launchAsync(F&& func, IExecutor* executor = IExecutor::getThreadPinnedExecutor()) noexcept {
            return launch(std::forward<F>(func), {.mode = LaunchMode::Async, .explicitExecutor = executor});
        }
        /**
         * @if RU
         * @brief Асинхронно запускает задачу на явном executor-е с аргументами.
         * @tparam F Фабрика задачи; @tparam Args Типы аргументов.
         * @param func Фабрика задачи.
         * @param executor Не владеющий executor, обязанный пережить задачу.
         * @param args Аргументы фабрики.
         * @return Поставленная AsyncTask либо ошибка запуска.
         * @endif
         * @if EN
         * @brief Asynchronously launches a task with arguments on an explicit executor.
         * @tparam F Task factory; @tparam Args Argument types.
         * @param func Task factory.
         * @param executor Non-owning executor that must outlive the task.
         * @param args Factory arguments.
         * @return Enqueued AsyncTask or a launch error.
         * @endif
         */
        template<typename F, typename... Args>
            requires CoroTaskFactory<F, Args...>
        [[nodiscard]] auto launchAsync(F&& func, IExecutor* executor, Args&&... args) noexcept {
            return launch(std::forward<F>(func), {.mode = LaunchMode::Async, .explicitExecutor = executor}, std::forward<Args>(args)...);
        }
        /**
         * @if RU
         * @brief Асинхронно запускает с аргументами на pinned executor текущего потока.
         * @tparam F Фабрика; @tparam FirstArg, Args Типы параметров фабрики.
         * @param func Фабрика задачи.
         * @param firstArg Первый аргумент (не executor pointer).
         * @param args Остальные аргументы.
         * @return Поставленная AsyncTask либо ошибка запуска.
         * @endif
         * @if EN
         * @brief Asynchronously launches with arguments on the current thread's pinned executor.
         * @tparam F Factory; @tparam FirstArg, Args Factory parameter types.
         * @param func Task factory.
         * @param firstArg First argument (not an executor pointer).
         * @param args Remaining arguments.
         * @return Enqueued AsyncTask or a launch error.
         * @endif
         */
        template<typename F, typename FirstArg, typename... Args>
            requires (!std::convertible_to<FirstArg, IExecutor*>)
                && CoroTaskFactory<F, FirstArg, Args...>
        [[nodiscard]] auto launchAsync(F&& func, FirstArg&& firstArg, Args&&... args) noexcept {
            return launch(
                std::forward<F>(func),
                {.mode = LaunchMode::Async, .explicitExecutor = IExecutor::getThreadPinnedExecutor()},
                std::forward<FirstArg>(firstArg),
                std::forward<Args>(args)...
            );
        }
        /**
         * @if RU
         * @brief Создаёт lazy child, не ставя его в очередь и не начиная выполнение.
         * @tparam F Фабрика задачи; @tparam Args Типы аргументов.
         * @param func Фабрика задачи.
         * @param args Аргументы фабрики.
         * @return Не запущенная AsyncTask.
         * @endif
         * @if EN
         * @brief Creates a lazy child without enqueueing or starting execution.
         * @tparam F Task factory; @tparam Args Argument types.
         * @param func Task factory.
         * @param args Factory arguments.
         * @return An unstarted AsyncTask.
         * @endif
         */
        template<typename F, typename... Args>
            requires CoroTaskFactory<F, Args...>
        [[nodiscard]] auto launchLazy(F&& func, Args&&... args) noexcept {
            return launch(std::forward<F>(func), {.mode = LaunchMode::Lazy, .explicitExecutor = nullptr}, std::forward<Args>(args)...);
        }

        /// @if RU
        /// @return true, если launch options этого scope имеют режим Immediate.
        /// @endif
        /// @if EN
        /// @return true if this scope's launch options use Immediate mode.
        /// @endif
        [[nodiscard]] bool canImmediateLaunched() const noexcept;

        /**
         * @if RU
         * @brief Выбирает handle для передачи управления после завершения coroutine.
         * @param coroutineHandle Frame завершающейся корутины; может быть уничтожен.
         * @return Parent handle для Immediate дочерней coroutine либо dispatcher.
         * @note Реализация уничтожает переданный coroutine frame до возврата.
         * @endif
         * @if EN
         * @brief Selects the handle to transfer control to after a coroutine completes.
         * @param coroutineHandle Frame of the completing coroutine; it may be destroyed.
         * @return Parent handle for an Immediate child coroutine, or the dispatcher.
         * @note The implementation destroys the supplied coroutine frame before returning.
         * @endif
         */
        [[nodiscard]] std::coroutine_handle<> getTransferCoro(std::coroutine_handle<> coroutineHandle) const noexcept;

        /**
         * @if RU
         * @brief Публикует, что joiner ожидает детей, и проверяет возможность завершения.
         * @param coroHandle Handle joiner-а, возвращаемый при уже пустом scope.
         * @return Handle joiner-а, если детей больше нет; nullptr, если ждать ещё.
         * @endif
         * @if EN
         * @brief Publishes that a joiner is waiting for children and checks whether it can complete.
         * @param coroHandle Joiner handle to return if the scope is already empty.
         * @return Joiner handle if no children remain; nullptr if it must keep waiting.
         * @endif
         */
        std::coroutine_handle<> checkCanDestroyed(std::coroutine_handle<> coroHandle) noexcept;

        /// @if RU
        /// Публикует handle родительской coroutine для child completion.
        /// @param coroHandle Не владеющий handle родительского frame.
        /// @endif
        /// @if EN
        /// Publishes the parent coroutine handle for child completion.
        /// @param coroHandle Non-owning handle of the parent frame.
        /// @endif
        void registerCoroHandle(std::coroutine_handle<> coroHandle) noexcept;
        /// @if RU
        /// Возвращает ранее зарегистрированный не владеющий handle либо пустой.
        /// @endif
        /// @if EN
        /// Returns the previously registered non-owning handle, or an empty handle.
        /// @endif
        [[nodiscard]] std::coroutine_handle<> getCoroHandle() const noexcept;
        /// @if RU
        /// Регистрирует coroutine/executor, которые нужно возобновить после завершения детей.
        /// @param coroHandle Handle joiner-а, который будет запланирован последним child.
        /// @param ctx Не владеющий executor; должен пережить ожидание всех детей.
        /// @endif
        /// @if EN
        /// Registers the coroutine/executor to resume after the children complete.
        /// @param coroHandle Joiner handle to be scheduled by the last child.
        /// @param ctx Non-owning executor; must outlive the wait for all children.
        /// @endif
        void registerJointerTaskHandle(std::coroutine_handle<> coroHandle, IExecutor *ctx) noexcept;
        /// @if RU
        /// Создаёт join coroutine, которая ждёт завершения детей текущего scope.
        /// @endif
        /// @if EN
        /// Creates a join coroutine that waits for this scope's children to complete.
        /// @endif
        JointerTask joinTask() noexcept { co_return; }

    protected:

        /**
         * @if RU
         * @brief Создаёт scope с указанной политикой и необязательным родителем.
         * @param options Настройки запуска дочерних задач.
         * @param parentScope Не владеющий родитель; если задан, его child counter
         *                    увеличивается и должен жить до разрушения этого scope.
         * @endif
         * @if EN
         * @brief Creates a scope with the specified policy and optional parent.
         * @param options Launch settings for child tasks.
         * @param parentScope Non-owning parent; if provided, its child counter
         *                    is incremented and it must outlive this scope.
         * @endif
         */
        explicit CoroScope(LaunchOptions options, CoroScope* parentScope = {}) noexcept
            : m_launch_options(options), m_parentScope(parentScope) {
            if (m_parentScope) {
                m_parentScope->incrementChildCount();
            }
        }

        /// @if RU
        /// Создаёт дочерний scope, увеличивая child counter родителя.
        /// @param options Политика запуска потомков созданного scope.
        /// @return Перемещаемый child scope, связанный с this.
        /// @endif
        /// @if EN
        /// Creates a child scope and increments the parent's child counter.
        /// @param options Launch policy for descendants of the created scope.
        /// @return Movable child scope associated with this scope.
        /// @endif
        CoroScope createChild(LaunchOptions options) noexcept;

    private:
        /// @if RU
        /// Увеличивает число живых потомков, не затрагивая маркер завершения.
        /// @endif
        /// @if EN
        /// Increments the number of live descendants without changing the completion marker.
        /// @endif
        void incrementChildCount() noexcept;
        /// @if RU
        /// Уменьшает parent counter; последний child после DEATH_SIGNAL будит joiner.
        /// @endif
        /// @if EN
        /// Decrements the parent counter; after DEATH_SIGNAL, the last child wakes the joiner.
        /// @endif
        void decrementChildCount() noexcept;

        /// @if RU
        /// Launch policy, унаследованная launch overload-ами для детей.
        /// @endif
        /// @if EN
        /// Launch policy inherited by child launch overloads.
        /// @endif
        LaunchOptions m_launch_options { .mode = LaunchMode::Async, .explicitExecutor = nullptr };

        // Инвариант завершения: DEATH_SIGNAL ставится joiner-ом один раз;
        // младшие биты считают живые child frame. Последний child, увидевший
        // предыдущее значение DEATH_SIGNAL + 1, будит зарегистрированный joiner.
        alignas(64) std::atomic<uint64_t> m_childCount{ 0 };
        // Не владеющий указатель; parent scope обязан пережить каждый child.
        CoroScope* m_parentScope{ nullptr };

        // Executor и join handle публикуются joiner-ом до установки DEATH_SIGNAL;
        // после публикации остаются неизменными до последнего child completion.
        alignas(64) IExecutor* m_threadExecutor{};
        std::atomic<void*> m_jointerTaskHandle{ nullptr };

        // Адрес handle родительской coroutine публикуется до запуска child-ов.
        alignas(64) std::atomic<void*> m_ownerHandle{ nullptr };
    };

    /**
     * @if RU
     * @brief Корневой scope, деструктор которого синхронно дожидается всех детей.
     *
     * joinAll() блокирует вызывающий OS-поток при наличии активных задач.
     * Для launch(options, ...) возвращается BlockingTask; остальные overload-ы
     * делегируют базовому CoroScope и возвращают AsyncTask.
     * @endif
     * @if EN
     * @brief Root scope whose destructor synchronously waits for all children.
     *
     * joinAll() blocks the calling OS thread while tasks are active. The
     * launch(options, ...) overload returns BlockingTask; other overloads
     * delegate to the base CoroScope and return AsyncTask.
     * @endif
     */
    class JoinableScope : CoroScope{
    public:
        /// @if RU
        /// Создаёт корневой scope с настройками Async без предустановленного executor-а.
        /// @endif
        /// @if EN
        /// Creates a root scope with Async settings and no preconfigured executor.
        /// @endif
        JoinableScope() : CoroScope {{.mode = LaunchMode::Async, .explicitExecutor = nullptr}, nullptr} {}

        /**
         * @if RU
         * @brief Создаёт задачу с явными options и оборачивает её в BlockingTask.
         * @tparam F Фабрика задачи; @tparam Args Типы аргументов.
         * @param f Фабрика задачи.
         * @param options Режим запуска и executor.
         * @param args Аргументы фабрики.
         * @return BlockingTask с той же ошибкой/результатом задачи.
         * @endif
         * @if EN
         * @brief Creates a task with explicit options and wraps it in BlockingTask.
         * @tparam F Task factory; @tparam Args Argument types.
         * @param f Task factory.
         * @param options Launch mode and executor.
         * @param args Factory arguments.
         * @return BlockingTask carrying the task's error/result.
         * @endif
         */
        template<typename F, typename... Args>
            requires CoroTaskFactory<F, Args...>
        [[nodiscard]] auto launch(F&& f, LaunchOptions options, Args&&... args) noexcept {
            return BlockingTask{std::move(CoroScope::launch(std::forward<F>(f), options, std::forward<Args>(args)...))};
        }

        /**
         * @if RU
         * @brief Делегирует создание задачи базовому scope с его параметрами запуска.
         * @tparam F Фабрика задачи без аргументов.
         * @param f Фабрика задачи.
         * @return AsyncTask результата или ошибки.
         * @endif
         * @if EN
         * @brief Delegates task creation to the base scope using its launch settings.
         * @tparam F Task factory with no arguments.
         * @param f Task factory.
         * @return AsyncTask containing the result or an error.
         * @endif
         */
        template<typename F>
            requires CoroTaskFactory<F>
        auto launch(F&& f) noexcept {
            return CoroScope::launch(std::forward<F>(f));
        }

        /**
         * @if RU
         * @brief Делегирует базовому scope запуск задачи с аргументами.
         * @tparam F Фабрика; @tparam FirstArg, Args Типы аргументов.
         * @param f Фабрика задачи.
         * @param firstArg Первый аргумент.
         * @param args Остальные аргументы.
         * @return AsyncTask результата или ошибки.
         * @endif
         * @if EN
         * @brief Delegates launching a task with arguments to the base scope.
         * @tparam F Factory; @tparam FirstArg, Args Argument types.
         * @param f Task factory.
         * @param firstArg First argument.
         * @param args Remaining arguments.
         * @return AsyncTask containing the result or an error.
         * @endif
         */
        template<typename F, typename FirstArg, typename... Args>
            requires (!std::same_as<std::remove_cvref_t<FirstArg>, LaunchOptions>)
                && CoroTaskFactory<F, FirstArg, Args...>
        auto launch(F&& f, FirstArg&& firstArg, Args&&... args) noexcept {
            return CoroScope::launch(
                std::forward<F>(f),
                std::forward<FirstArg>(firstArg),
                std::forward<Args>(args)...
            );
        }

        /// @if RU
        /// @brief Создаёт задачу в Immediate режиме.
        ///
        /// Выполнение корутины начинается синхронно в вызывающем контексте до первой точки приостановки.
        ///
        /// @warning **КРИТИЧЕСКОЕ ПРЕДУПРЕЖДЕНИЕ ПО БЕЗОПАСНОСТИ:**
        /// Использовать оператор `co_await` внутри этой задачи разрешается **строго в рамках одного и того же потока**
        /// (например, совместно с ThreadPinnedExecutor / OneThreadExecutor). Ни в коем случае не допускайте миграции
        /// контекста выполнения на другие потоки или пулы потоков внутри Immediate-задач. Смена потока возобновления
        /// нарушит иерархию Structured Concurrency и **приведёт к неопределённому поведению (UB)**.
        ///
        /// @tparam F Фабрика задачи; @tparam Args Типы её аргументов.
        /// @param func Фабрика задачи.
        /// @param args Аргументы фабрики.
        /// @return AsyncTask результата или ошибки.
        /// @endif
        /// @if EN
        /// @brief Creates a task in Immediate mode.
        ///
        /// Coroutine execution starts synchronously in the calling context until the first suspension point.
        ///
        /// @warning **CRITICAL SAFETY WARNING:**
        /// Using the `co_await` operator inside this task is allowed **strictly within the same thread context**
        /// (e.g., in conjunction with a ThreadPinnedExecutor / OneThreadExecutor). Thread migration to other threads
        /// or thread pools inside an Immediate task is strictly prohibited. Changing the execution context to a
        /// different thread will break Structured Concurrency invariants and **lead to undefined behavior (UB)**.
        ///
        /// @tparam F Task factory; @tparam Args Its argument types.
        /// @param func Task factory.
        /// @param args Factory arguments.
        /// @return AsyncTask containing the result or an error.
        /// @endif
        template<typename F, typename... Args>
            requires CoroTaskFactory<F, Args...>
        [[nodiscard]] auto launchImmediate(F&& func, Args&&... args) noexcept {
            return launch(std::forward<F>(func), {.mode = LaunchMode::Immediate, .explicitExecutor = nullptr}, std::forward<Args>(args)...);
        }

        /// @if RU
        /// @brief Создаёт задачу в Async режиме на указанном/pinned executor-е.
        /// @tparam F Фабрика задачи.
        /// @param func Фабрика задачи.
        /// @param executor Не владеющий executor либо nullptr, если TLS executor отсутствует.
        /// @return AsyncTask результата или ошибки постановки.
        /// @endif
        /// @if EN
        /// @brief Creates a task in Async mode on the specified or pinned executor.
        /// @tparam F Task factory.
        /// @param func Task factory.
        /// @param executor Non-owning executor, or nullptr if no TLS executor is available.
        /// @return AsyncTask with the result or an enqueue error.
        /// @endif
        template<typename F>
            requires CoroTaskFactory<F>
        [[nodiscard]] auto launchAsync(F&& func, IExecutor* executor = IExecutor::getThreadPinnedExecutor()) noexcept {
            return launch(std::forward<F>(func), {.mode = LaunchMode::Async, .explicitExecutor = executor});
        }
        /// @if RU
        /// @brief Создаёт задачу в Async режиме на явном executor-е.
        /// @tparam F Фабрика; @tparam Args Типы аргументов.
        /// @param func Фабрика задачи.
        /// @param executor Не владеющий executor.
        /// @param args Аргументы фабрики.
        /// @return AsyncTask результата или ошибки постановки.
        /// @endif
        /// @if EN
        /// @brief Creates a task in Async mode on an explicit executor.
        /// @tparam F Factory; @tparam Args Argument types.
        /// @param func Task factory.
        /// @param executor Non-owning executor.
        /// @param args Factory arguments.
        /// @return AsyncTask with the result or an enqueue error.
        /// @endif
        template<typename F, typename... Args>
            requires CoroTaskFactory<F, Args...>
        [[nodiscard]] auto launchAsync(F&& func, IExecutor* executor, Args&&... args) noexcept {
            return launch(std::forward<F>(func), {.mode = LaunchMode::Async, .explicitExecutor = executor}, std::forward<Args>(args)...);
        }
        /// @if RU
        /// @brief Создаёт задачу в Async режиме на pinned executor-е с аргументами.
        /// @tparam F Фабрика; @tparam FirstArg, Args Типы аргументов.
        /// @param func Фабрика задачи.
        /// @param firstArg Первый аргумент, не являющийся executor pointer.
        /// @param args Остальные аргументы.
        /// @return AsyncTask результата или ошибки постановки.
        /// @endif
        /// @if EN
        /// @brief Creates a task in Async mode on the pinned executor with arguments.
        /// @tparam F Factory; @tparam FirstArg, Args Argument types.
        /// @param func Task factory.
        /// @param firstArg First argument, which is not an executor pointer.
        /// @param args Remaining arguments.
        /// @return AsyncTask with the result or an enqueue error.
        /// @endif
        template<typename F, typename FirstArg, typename... Args>
            requires (!std::convertible_to<FirstArg, IExecutor*>)
                && CoroTaskFactory<F, FirstArg, Args...>
        [[nodiscard]] auto launchAsync(F&& func, FirstArg&& firstArg, Args&&... args) noexcept {
            return launch(
                std::forward<F>(func),
                {.mode = LaunchMode::Async, .explicitExecutor = IExecutor::getThreadPinnedExecutor()},
                std::forward<FirstArg>(firstArg),
                std::forward<Args>(args)...
            );
        }
        /// @if RU
        /// @brief Создаёт задачу в Lazy режиме.
        /// @tparam F Фабрика; @tparam Args Типы аргументов.
        /// @param func Фабрика задачи.
        /// @param args Аргументы фабрики.
        /// @return Не запущенная AsyncTask.
        /// @endif
        /// @if EN
        /// @brief Creates a task in Lazy mode.
        /// @tparam F Factory; @tparam Args Argument types.
        /// @param func Task factory.
        /// @param args Factory arguments.
        /// @return An unstarted AsyncTask.
        /// @endif
        template<typename F, typename... Args>
            requires CoroTaskFactory<F, Args...>
        [[nodiscard]] auto launchLazy(F&& func, Args&&... args) noexcept {
            return launch(std::forward<F>(func), {.mode = LaunchMode::Lazy, .explicitExecutor = nullptr}, std::forward<Args>(args)...);
        }

        /**
         * @if RU
         * @brief Дожидается уничтожения всех дочерних coroutine frame.
         *
         * Устанавливает TLS BlockingExecutor на время ожидания. Метод блокирует
         * текущий поток; вызов из дочернего worker-потока этого же scope может
         * привести к deadlock и не является поддерживаемым шаблоном использования.
         * @endif
         * @if EN
         * @brief Waits for all child coroutine frames to be destroyed.
         *
         * Installs a TLS BlockingExecutor while waiting. This method blocks the
         * current thread; calling it from a child worker thread of the same
         * scope may deadlock and is not a supported usage pattern.
         * @endif
         */
        void joinAll() {
            auto threadContextGuard = m_blockingExecutor.initializeThreadContext();
            registerJointerTaskHandle(std::noop_coroutine(), &m_blockingExecutor);
            if (checkCanDestroyed(std::noop_coroutine()) == nullptr) {
                m_blockingExecutor.waitForSignal();
            }
        }

        /**
         * @if RU
         * @brief Дожидается всех дочерних задач, затем разрушает scope.
         * @endif
         * @if EN
         * @brief Waits for all child tasks, then destroys the scope.
         * @endif
         */
        ~JoinableScope() {
            joinAll();
        }
    private:
        BlockingExecutor m_blockingExecutor;
    };

} // namespace shuttle::async

#include <async/async_task.hpp>

#endif //SHUTTLEENGINE_COROSCOPE_HPP
