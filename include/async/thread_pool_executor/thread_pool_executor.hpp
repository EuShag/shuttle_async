//
// Created by Shagu on 13.10.2026.
//

#ifndef SHUTTLEENGINE_THREADPOOLEXECUTOR_HPP
#define SHUTTLEENGINE_THREADPOOLEXECUTOR_HPP

#include <concurrentqueue.h>
#include <async/thread_pool_executor/coroutine_work_stealing_queue.hpp>
#include <async/thread_pool_executor/notifier.hpp>
#include <atomic>
#include <optional>
#include <vector>
#include <functional>

#include <async/i_executor.hpp>
#include <async/mi_allocator.hpp>
#include <async/thread_pool_executor/thread_pool_dispatcher_task.hpp>

namespace shuttle::async {

    class ThreadPoolExecutor;

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
     * worker-а. Callback перемещается внутрь пула.
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
     * one of its own workers. The callback is moved into the pool.
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
         * @return Владеющий executor либо allocation_failure, invalid_worker_count
         *         или worker_start_failed.
         * @endif
         * @if EN
         * @brief Creates a pool of the specified size and starts its workers.
         * @param numWorkers Number of workers; zero yields invalid_worker_count.
         * @param crushCallback Handler for worker dispatcher errors.
         * @return Owning executor, or allocation_failure, invalid_worker_count,
         *         or worker_start_failed.
         * @endif
         */
        [[nodiscard]] static Result<MiUniquePtr<ThreadPoolExecutor>> create(
            size_t numWorkers,
            WorkerCrashCallback&& crushCallback) noexcept;

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
         * @endif
         */
        explicit ThreadPoolExecutor(
            size_t numWorkers,
            std::vector<std::jthread, MiAllocator<std::jthread>> && threads,
            std::vector<WorkerContext, MiAllocator<WorkerContext>> && workers,
            moodycamel::ConcurrentQueue<std::coroutine_handle<>>&& incomingQueue,
            WorkerCrashCallback&& callback) noexcept :
        m_incomingQueue {std::move(incomingQueue)},
        m_threads{std::move(threads)},
        m_workers{std::move(workers)},
        m_numWorkers{numWorkers},
        m_crashCallback{std::move(callback)} {}

    private:
        friend class WorkerContext::WorkerExecutor;

        /// @if RU
        /// Запускает worker-потоки после подготовки ресурсов.
        /// @endif
        /// @if EN
        /// Starts worker threads after resources have been prepared.
        /// @endif
        [[nodiscard]] Status init() noexcept;
        /// @if RU
        /// Уведомляет одного ожидающего worker-а, если такие есть.
        /// @endif
        /// @if EN
        /// Notifies one waiting worker, if any.
        /// @endif
        void notifyWorkAvailable() noexcept;

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
        /// Число worker-ов, которые готовятся ждать или уже ожидают работу.
        /// @endif
        /// @if EN
        /// Number of workers preparing to wait or already waiting for work.
        /// @endif
        alignas(64) std::atomic<uint64_t> m_sleepingWorkerCount{ 0 };

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
        /// Callback ошибки dispatcher-а, перемещённый внутрь executor-а.
        /// @endif
        /// @if EN
        /// Dispatcher error callback moved into the executor.
        /// @endif
        WorkerCrashCallback m_crashCallback{ nullptr };
    };
}

#endif //SHUTTLEENGINE_THREADPOOLEXECUTOR_HPP
