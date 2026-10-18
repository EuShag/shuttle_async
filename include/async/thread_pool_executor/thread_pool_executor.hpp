//
// Created by Shagu on 13.10.2026.
//

#ifndef SHUTTLEENGINE_THREADPOOLEXECUTOR_HPP
#define SHUTTLEENGINE_THREADPOOLEXECUTOR_HPP

#include <concurrentqueue.h>
#include <async/thread_pool_executor/coroutine_work_stealing_queue.hpp>
#include <async/thread_pool_executor/notifier.hpp>
#include <array>
#include <atomic>
#include <optional>
#include <vector>
#include <functional>
#include <memory>

#include <async/i_executor.hpp>
#include <async/mi_allocator.hpp>
#include <async/thread_pool_executor/thread_pool_dispatcher_task.hpp>

namespace shuttle::async {

    class ThreadPoolExecutor;

    /**
     * @if RU
     * @brief Необязательные lock-free счётчики планирования пула.
     *
     * Объект предоставляется пользователем и не владеется executor-ом; он должен
     * оставаться жив до остановки и завершения worker-ов. При включённых worker
     * счётчиках numWorkers не должен превышать MaxWorkerCount. Счётчики монотонны
     * и предназначены для диагностики, а не для управления исполнением.
     * @endif
     * @if EN
     * @brief Optional lock-free counters for pool scheduling.
     *
     * The object is provided by the user and is not owned by the executor; it
     * must remain alive until shutdown and all workers have finished. When
     * worker counters are enabled, numWorkers must not exceed MaxWorkerCount.
     * Counters are monotonic and intended for diagnostics, not execution control.
     * @endif
     */
    struct ThreadPoolDiagnostics {
        /// @if RU
        /// Максимальный поддерживаемый набор счётчиков worker-ов.
        /// @endif
        /// @if EN
        /// Maximum supported set of worker counters.
        /// @endif
        static constexpr size_t MaxWorkerCount = 64;

        /// @if RU
        /// Атомарные статистические счётчики одного worker-а.
        /// @endif
        /// @if EN
        /// Atomic statistics counters for one worker.
        /// @endif
        struct alignas(64) WorkerCounters {
            /// @if RU
            /// Число успешных извлечений из локальной deque.
            /// @endif
            /// @if EN
            /// Number of successful dequeues from the local deque.
            /// @endif
            std::atomic<uint64_t> localDequePops{0};
            /// @if RU
            /// Число извлечений входящих задач.
            /// @endif
            /// @if EN
            /// Number of dequeued incoming tasks.
            /// @endif
            std::atomic<uint64_t> incomingDequeues{0};
            /// @if RU
            /// Число попыток поиска задач у других worker-ов.
            /// @endif
            /// @if EN
            /// Number of attempts to find tasks on other workers.
            /// @endif
            std::atomic<uint64_t> stealProbes{0};
            /// @if RU
            /// Число успешно украденных задач.
            /// @endif
            /// @if EN
            /// Number of successfully stolen tasks.
            /// @endif
            std::atomic<uint64_t> successfulSteals{0};
            /// @if RU
            /// Число переходов к ожиданию работы.
            /// @endif
            /// @if EN
            /// Number of transitions to waiting for work.
            /// @endif
            std::atomic<uint64_t> idleWaits{0};
            /// @if RU
            /// Число возвратов из ожидания.
            /// @endif
            /// @if EN
            /// Number of returns from waiting.
            /// @endif
            std::atomic<uint64_t> waitReturns{0};
        };

        /// @if RU
        /// Число локальных постановок из worker-потоков.
        /// @endif
        /// @if EN
        /// Number of local enqueues from worker threads.
        /// @endif
        std::atomic<uint64_t> localEnqueues{0};
        /// @if RU
        /// Число постановок во входящую глобальную очередь.
        /// @endif
        /// @if EN
        /// Number of enqueues to the global incoming queue.
        /// @endif
        std::atomic<uint64_t> incomingEnqueues{0};
        /// @if RU
        /// Число отправленных уведомлений о работе.
        /// @endif
        /// @if EN
        /// Number of work notifications sent.
        /// @endif
        std::atomic<uint64_t> workNotifications{0};
        /// @if RU
        /// Счётчики каждого возможного worker-а; неиспользуемые слоты остаются нулевыми.
        /// @endif
        /// @if EN
        /// Counters for every possible worker; unused slots remain zero.
        /// @endif
        std::array<WorkerCounters, MaxWorkerCount> workers{};
    };

    /**
     * @if RU
     * @brief Контекст worker-а пула: локальные очереди, dispatcher и executor продолжений.
     *
     * Это внутренняя структура реализации, но является именованным публичным типом.
     * Содержимое не предназначено для пользовательского изменения.
     * @endif
     * @if EN
     * @brief Pool worker context: local queues, dispatcher, and continuation executor.
     *
     * This is an internal implementation structure, but it is a named public
     * type. Its contents are not intended to be modified by users.
     * @endif
     */
    struct alignas(64) WorkerContext {
        /// @if RU
        /// Индекс worker-а в пуле.
        /// @endif
        /// @if EN
        /// Worker index in the pool.
        /// @endif
        size_t m_id{ 0 };
        /// @if RU
        /// Состояние генератора псевдослучайного выбора victim для steal.
        /// @endif
        /// @if EN
        /// State of the pseudo-random generator used to choose a steal victim.
        /// @endif
        uint32_t m_randomState{ 0 };

        /// @if RU
        /// Локальная ограниченная deque, откуда worker забирает свои задачи.
        /// @endif
        /// @if EN
        /// Bounded local deque from which the worker takes its tasks.
        /// @endif
        CoroutineWorkStealingQueue<> m_queue{};
        /// @if RU
        /// Handle dispatcher coroutine данного worker-а.
        /// @endif
        /// @if EN
        /// Dispatcher coroutine handle for this worker.
        /// @endif
        std::coroutine_handle<> m_dispatcher{};

        /// @if RU
        /// Очередь служебных continuation-задач.
        /// @endif
        /// @if EN
        /// Queue of service continuation tasks.
        /// @endif
        moodycamel::ConcurrentQueue<std::coroutine_handle<>> m_serviceQueue;
        /// @if RU
        /// Токен потребителя служебной очереди, создаваемый на worker-е.
        /// @endif
        /// @if EN
        /// Consumer token for the service queue, created on the worker.
        /// @endif
        std::optional<moodycamel::ConsumerToken> m_serviceQueueConsumerToken;
        /// @if RU
        /// Токен потребителя общей входящей очереди.
        /// @endif
        /// @if EN
        /// Consumer token for the shared incoming queue.
        /// @endif
        std::optional<moodycamel::ConsumerToken> m_incomingQueueConsumerToken;

        /// @if RU
        /// Приблизительное число элементов служебной очереди.
        /// @endif
        /// @if EN
        /// Approximate number of items in the service queue.
        /// @endif
        alignas(64) std::atomic<uint64_t> m_serviceQueueSize{ 0 };

        /// @if RU
        /// Executor, адресованный конкретному worker-у для pinned continuations.
        /// @endif
        /// @if EN
        /// Executor addressed to a specific worker for pinned continuations.
        /// @endif
        class WorkerExecutor final : public IExecutor {
        public:
            /// @if RU
            /// Создаёт неинициализированный executor; используется как поле контекста.
            /// @endif
            /// @if EN
            /// Creates an uninitialized executor; used as a context member.
            /// @endif
            WorkerExecutor() = default;
            /**
             * @if RU
             * @brief Связывает executor продолжений с worker-ом и пулом.
             * @param ctx Не владеющий контекст worker-а.
             * @param exec Не владеющий пул; оба объекта должны пережить executor.
             * @endif
             * @if EN
             * @brief Binds the continuation executor to a worker and its pool.
             * @param ctx Non-owning worker context.
             * @param exec Non-owning pool; both objects must outlive the executor.
             * @endif
             */
            explicit WorkerExecutor(WorkerContext* ctx, ThreadPoolExecutor* exec) noexcept
                : workerContext{ctx}, threadPoolExecutor{exec} {}

            /**
             * @if RU
             * @brief Ставит continuation в служебную очередь конкретного worker-а.
             * @param coro Handle корутины; владение frame не передаётся.
             * @return Ошибка queue_enqueue_failed, если очередь отказала.
             * @endif
             * @if EN
             * @brief Enqueues a continuation on the service queue of a specific worker.
             * @param coro Coroutine handle; ownership of the frame is not transferred.
             * @return queue_enqueue_failed if the queue rejects the enqueue.
             * @endif
             */
            [[nodiscard]] Status execute(std::coroutine_handle<> coro) noexcept override;
        private:
            /// @if RU
            /// Не владеющий адрес контекста worker-а.
            /// @endif
            /// @if EN
            /// Non-owning address of the worker context.
            /// @endif
            WorkerContext* workerContext{ nullptr };
            /// @if RU
            /// Не владеющий адрес пула, который маршрутизирует wakeup.
            /// @endif
            /// @if EN
            /// Non-owning address of the pool that routes wakeups.
            /// @endif
            ThreadPoolExecutor* threadPoolExecutor{ nullptr };
            /// @if RU
            /// Сигнализирует пулу о задаче, поставленной в service queue.
            /// @endif
            /// @if EN
            /// Notifies the pool about a task enqueued in the service queue.
            /// @endif
            void wakeUpWorker() const noexcept;
        } m_continuation;
    };

    /**
     * @if RU
     * @brief Многопоточный executor с локальными deque и work-stealing.
     *
     * Создаётся фабрикой create(), которая запускает worker-ы. Внешние отправители
     * используют общую входящую очередь; отправка из worker-а добавляет работу в
     * его локальную очередь. Деструктор останавливает и присоединяет потоки.
     * Все coroutine handles не владеющие: frame должны сохраняться вызывающей
     * стороной до завершения задачи; отправители не должны конкурентно обращаться
     * к executor-у во время его разрушения, и пул нельзя разрушать из собственного
     * worker-а. Callback перемещается внутрь пула,
     * diagnostics остаётся не владеющим указателем и должен пережить worker-ы.
     * @endif
     * @if EN
     * @brief Multithreaded executor with local deques and work stealing.
     *
     * Created by the create() factory, which starts the workers. External
     * submitters use the shared incoming queue; submissions from a worker go to
     * its local queue. Destruction stops and joins the threads. All coroutine
     * handles are non-owning: frames must remain alive under the caller's
     * ownership until task completion. Submitters must not access the executor
     * concurrently with its destruction, and the pool must not be destroyed by
     * one of its own workers. The callback is moved into the pool; diagnostics
     * remains a non-owning pointer and must outlive the workers.
     * @endif
     */
    class ThreadPoolExecutor final : public IExecutor {
    public:
        /// @if RU
        /// Move-only обработчик ошибки старта dispatcher-а worker-а.
        /// @endif
        /// @if EN
        /// Move-only handler for a worker dispatcher startup error.
        /// @endif
        using WorkerCrashCallback = std::move_only_function<void (size_t id, Error)>;

        /**
         * @if RU
         * @brief Создаёт пул с количеством worker-ов по hardware_concurrency().
         * @param crushCallback Вызывается при ошибке dispatcher-а; пустой callback
         *                     не следует использовать при возможной ошибке dispatcher-а.
         * @return Владеющий executor либо invalid_worker_count, allocation_failure
         *         или ошибка создания worker-ов (включая hardware_concurrency == 0).
         * @endif
         * @if EN
         * @brief Creates a pool with a worker count based on hardware_concurrency().
         * @param crushCallback Called if a dispatcher fails; a null callback
         *                     should not be used if dispatcher failure is possible.
         * @return Owning executor, or invalid_worker_count, allocation_failure,
         *         or a worker-creation error (including hardware_concurrency == 0).
         * @endif
         */
        [[nodiscard]] static Result<MiUniquePtr<ThreadPoolExecutor>> create(
            WorkerCrashCallback&& crushCallback = nullptr) noexcept {
            return create(std::thread::hardware_concurrency(), std::move(crushCallback));
        }

        /**
         * @if RU
         * @brief Создаёт пул заданного размера и запускает его worker-ы.
         * @param numWorkers Число worker-ов; ноль даёт invalid_worker_count.
         * @param crushCallback Обработчик ошибки dispatcher-а worker-а.
         * @param diagnostics Не владеющая статистика или nullptr; должна пережить пул.
         * @return Владеющий executor либо allocation_failure, invalid_worker_count
         *         или worker_start_failed.
         * @endif
         * @if EN
         * @brief Creates a pool of the specified size and starts its workers.
         * @param numWorkers Number of workers; zero yields invalid_worker_count.
         * @param crushCallback Handler for worker dispatcher errors.
         * @param diagnostics Non-owning statistics object or nullptr; must outlive the pool.
         * @return Owning executor, or allocation_failure, invalid_worker_count,
         *         or worker_start_failed.
         * @endif
         */
        [[nodiscard]] static Result<MiUniquePtr<ThreadPoolExecutor>> create(
            size_t numWorkers,
            WorkerCrashCallback&& crushCallback,
            ThreadPoolDiagnostics* diagnostics = nullptr) noexcept;

        /// @if RU
        /// Останавливает пул и ожидает завершения всех принадлежащих ему jthread.
        /// @endif
        /// @if EN
        /// Stops the pool and waits for all owned jthreads to finish.
        /// @endif
        ~ThreadPoolExecutor() override;

        /**
         * @if RU
         * @brief Ставит coroutine handle в пул.
         * @param coro Валидный handle; coroutine frame остаётся во владении вызывающего.
         * @return Ошибка для пустого handle, остановленного пула или отказа очереди.
         * @note Thread safety: поддерживает вызовы извне и из worker-ов пула.
         * @endif
         * @if EN
         * @brief Enqueues a coroutine handle in the pool.
         * @param coro Valid handle; the caller retains ownership of the coroutine frame.
         * @return Error for an empty handle, a stopped pool, or a queue rejection.
         * @note Thread safety: supports calls from outside the pool and from its workers.
         * @endif
         */
        [[nodiscard]] Status execute(std::coroutine_handle<> coro) override;

        /**
         * @if RU
         * @brief Явно отправляет handle в общую входящую очередь.
         * @param handle Coroutine handle без передачи владения frame.
         * @return Ошибка queue_enqueue_failed, если очередь не приняла задачу.
         * @endif
         * @if EN
         * @brief Explicitly submits a handle to the shared incoming queue.
         * @param handle Coroutine handle; ownership of the frame is not transferred.
         * @return queue_enqueue_failed if the queue rejects the task.
         * @endif
         */
        [[nodiscard]] Status enqueueIncoming(std::coroutine_handle<> handle) noexcept;

        /**
         * @if RU
         * @brief Сигнализирует worker-ам о потенциально доступной работе.
         * @param size Идентификатор worker-а для которого запрашивается пробуждение.
         * @note В текущей реализации параметр не используется для адресного пробуждения.
         * @endif
         * @if EN
         * @brief Notifies workers that work may be available.
         * @param size Worker identifier for which a wakeup is requested.
         * @note The current implementation does not use the parameter for targeted wakeups.
         * @endif
         */
        void signalWorkerWakeup(size_t size);

        /// @if RU
        /// Создаёт неинициализированную оболочку; используйте фабрику create().
        /// @endif
        /// @if EN
        /// Creates an uninitialized wrapper; use the create() factory.
        /// @endif
        explicit ThreadPoolExecutor() = default;

        /**
         * @if RU
         * @brief Создаёт executor из предварительно подготовленных ресурсов.
         *
         * Конструктор предназначен для фабрики и низкоуровневого использования:
         * потоки не запускаются до init(). Все переданные контейнеры перемещаются.
         * @param numWorkers Число контекстов worker-ов.
         * @param threads Вектор потоков, передаваемый во владение executor-у.
         * @param workers Вектор контекстов worker-ов, передаваемый во владение.
         * @param incomingQueue Подготовленная общая очередь.
         * @param callback Обработчик ошибок worker-ов.
         * @param diagnostics Не владеющий указатель на счётчики.
         * @endif
         * @if EN
         * @brief Constructs an executor from pre-initialized resources.
         *
         * Intended for the factory and low-level use: threads are not started
         * until init(). All supplied containers are moved.
         * @param numWorkers Number of worker contexts.
         * @param threads Vector of threads transferred to the executor.
         * @param workers Vector of worker contexts transferred to the executor.
         * @param incomingQueue Prepared shared queue.
         * @param callback Worker error handler.
         * @param diagnostics Non-owning pointer to counters.
         * @endif
         */
        explicit ThreadPoolExecutor(
            size_t numWorkers,
            std::vector<std::jthread, MiAllocator<std::jthread>> && threads,
            std::vector<WorkerContext, MiAllocator<WorkerContext>> && workers,
            moodycamel::ConcurrentQueue<std::coroutine_handle<>>&& incomingQueue,
            WorkerCrashCallback&& callback,
            ThreadPoolDiagnostics* diagnostics = nullptr) noexcept :
        m_incomingQueue {std::move(incomingQueue)},
        m_threads{std::move(threads)},
        m_workers{std::move(workers)},
        m_numWorkers{numWorkers},
        m_diagnostics{diagnostics},
        m_crashCallback{std::move(callback)} {}

    private:
        friend class WorkerContext::WorkerExecutor;

        /// @if RU
        /// Нижние 32 бита sleep state хранят резервированные wakeup-ы.
        /// @endif
        /// @if EN
        /// The low 32 bits of the sleep state store reserved wakeups.
        /// @endif
        static constexpr uint64_t SleepReservationMask = 0xffffffffULL;
        /// @if RU
        /// Инкремент верхней половины за одного спящего worker-а.
        /// @endif
        /// @if EN
        /// Increment of the upper half for one sleeping worker.
        /// @endif
        static constexpr uint64_t SleepingWorkerIncrement = 1ULL << 32;

        /// @if RU
        /// Запускает worker-потоки после подготовки ресурсов.
        /// @endif
        /// @if EN
        /// Starts worker threads after resources have been prepared.
        /// @endif
        [[nodiscard]] Status init() noexcept;
        /// @if RU
        /// Уведомляет одного ожидающего worker-а при наличии свободной резервации.
        /// @endif
        /// @if EN
        /// Notifies one waiting worker if a wakeup reservation is available.
        /// @endif
        void notifyWorkAvailable() noexcept;
        /// @if RU
        /// Снимает состояние сна и использованную резервацию после пробуждения.
        /// @endif
        /// @if EN
        /// Clears the sleep state and consumed reservation after wakeup.
        /// @endif
        void leaveSleepState() noexcept;

        /// @if RU
        /// Текущий WorkerContext этого OS-потока; используется fast path execute().
        /// @endif
        /// @if EN
        /// Current WorkerContext for this OS thread; used by execute()'s fast path.
        /// @endif
        inline static thread_local WorkerContext* m_currentWorker{ nullptr };

        /// @if RU
        /// Точка входа принадлежащего worker-потоку цикла.
        /// @endif
        /// @if EN
        /// Entry point for the owned worker-thread loop.
        /// @endif
        void workerLoop(size_t id) noexcept;

        /// @if RU
        /// Coroutine диспетчера планирования одного worker-а.
        /// @endif
        /// @if EN
        /// Scheduling dispatcher coroutine for one worker.
        /// @endif
        DispatcherTask runDispatcher(WorkerContext *ctx);

        /// @if RU
        /// MPMC очередь для внешних задач и переполнения local deque.
        /// @endif
        /// @if EN
        /// MPMC queue for external tasks and local-deque overflow.
        /// @endif
        moodycamel::ConcurrentQueue<std::coroutine_handle<>> m_incomingQueue;
        /// @if RU
        /// Приблизительный счётчик входящих элементов перед dequeue.
        /// @endif
        /// @if EN
        /// Approximate count of incoming items before dequeue.
        /// @endif
        alignas(64) std::atomic<uint64_t> m_incomingCount{ 0 };
        /// @if RU
        /// Верхняя половина считает sleeping workers, нижняя — reserved wakeups.
        /// @endif
        /// @if EN
        /// Upper half counts sleeping workers; lower half counts reserved wakeups.
        /// @endif
        alignas(64) std::atomic<uint64_t> m_sleepState{ 0 };

        /// @if RU
        /// Generation-based notification для спящих dispatcher-ов.
        /// @endif
        /// @if EN
        /// Generation-based notification for sleeping dispatchers.
        /// @endif
        Notifier m_notifier{};

        /// @if RU
        /// Владеющие jthread; очистка в деструкторе останавливает и join-ит их.
        /// @endif
        /// @if EN
        /// Owned jthreads; destruction stops and joins them.
        /// @endif
        std::vector<std::jthread, MiAllocator<std::jthread>> m_threads;

        // Вектор монолитных структур воркеров теперь безопасен, так как элементы выровнены по 64 байта
        /// @if RU
        /// Контексты worker-ов стабильны по адресу на протяжении жизни их потоков.
        /// @endif
        /// @if EN
        /// Worker contexts remain at stable addresses for the lifetime of their threads.
        /// @endif
        std::vector<WorkerContext, MiAllocator<WorkerContext>> m_workers;

        /// @if RU
        /// После установки блокирует новые execute(); worker-ы завершаются по этому флагу.
        /// @endif
        /// @if EN
        /// Once set, blocks new execute() calls; workers stop on this flag.
        /// @endif
        alignas(64) std::atomic<bool> m_stopping{ false };
        /// @if RU
        /// Начальная точка выбора очередного worker-а/victim.
        /// @endif
        /// @if EN
        /// Starting point for choosing the next worker/victim.
        /// @endif
        std::atomic<size_t> m_nextWorkerIndex{ 0 };
        /// @if RU
        /// Число worker-ов, принадлежащих пулу.
        /// @endif
        /// @if EN
        /// Number of workers owned by the pool.
        /// @endif
        size_t m_numWorkers{0};
        /// @if RU
        /// Не владеющие диагностические счётчики.
        /// @endif
        /// @if EN
        /// Non-owning diagnostics counters.
        /// @endif
        ThreadPoolDiagnostics* m_diagnostics{nullptr};
        /// @if RU
        /// Callback ошибки dispatcher-а, перемещённый внутрь executor-а.
        /// @endif
        /// @if EN
        /// Dispatcher error callback moved into the executor.
        /// @endif
        WorkerCrashCallback m_crashCallback{ nullptr };
    };
}

#endif //SHUTTLEENGINE_THREADPOOLEXECUTOR_HPP
