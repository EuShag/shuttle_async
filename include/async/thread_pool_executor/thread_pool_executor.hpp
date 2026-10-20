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
     * @brief Pool worker context: local queues, dispatcher, and continuation executor.
     *
     * This is an internal implementation structure, but it is a named public
     * type. Its contents are not intended to be modified by users.
     */
    struct alignas(64) WorkerContext {
        /// Worker index in the pool.
        size_t m_id{ 0 };
        /// State of the pseudo-random generator used to choose a steal victim.
        uint32_t m_randomState{ 0 };

        /// Bounded local deque from which the worker takes its tasks.
        CoroutineWorkStealingQueue<> m_queue{};
        /// Dispatcher coroutine handle for this worker.
        std::coroutine_handle<> m_dispatcher{};

        /// Queue of service continuation tasks.
        moodycamel::ConcurrentQueue<std::coroutine_handle<>> m_serviceQueue;
        /// Consumer token for the service queue, created on the worker.
        std::optional<moodycamel::ConsumerToken> m_serviceQueueConsumerToken;
        /// Consumer token for the shared incoming queue.
        std::optional<moodycamel::ConsumerToken> m_incomingQueueConsumerToken;

        /// Approximate number of items in the service queue.
        alignas(64) std::atomic<uint64_t> m_serviceQueueSize{ 0 };

        /// Executor addressed to a specific worker for pinned continuations.
        class WorkerExecutor final : public IExecutor {
        public:
            /// Creates an uninitialized executor; used as a context member.
            WorkerExecutor() = default;
            /**
             * @brief Binds the continuation executor to a worker and its pool.
             * @param ctx Non-owning worker context.
             * @param exec Non-owning pool; both objects must outlive the executor.
             */
            explicit WorkerExecutor(WorkerContext* ctx, ThreadPoolExecutor* exec) noexcept
                : workerContext{ctx}, threadPoolExecutor{exec} {}

            /**
             * @brief Enqueues a continuation on the service queue of a specific worker.
             * @param coro Coroutine handle; ownership of the frame is not transferred.
             * @return queue_enqueue_failed if the queue rejects the enqueue.
             */
            [[nodiscard]] Status execute(std::coroutine_handle<> coro) noexcept override;
        private:
            /// Non-owning address of the worker context.
            WorkerContext* workerContext{ nullptr };
            /// Non-owning address of the pool that routes wakeups.
            ThreadPoolExecutor* threadPoolExecutor{ nullptr };
            /// Notifies the pool about a task enqueued in the service queue.
            void wakeUpWorker() const noexcept;
        } m_continuation;
    };

    /**
     * @brief Multithreaded executor with local deques and work stealing.
     *
     * Created by the create() factory, which starts the workers. External
     * submitters use the shared incoming queue; submissions from a worker go to
     * its local queue. Destruction stops and joins the threads. All coroutine
     * handles are non-owning: frames must remain alive under the caller's
     * ownership until task completion. Submitters must not access the executor
     * concurrently with its destruction, and the pool must not be destroyed by
     * one of its own workers. The callback is moved into the pool.
     */
    class ThreadPoolExecutor final : public IExecutor {
    public:
        /// Move-only handler for a worker dispatcher startup error.
        using WorkerCrashCallback = std::move_only_function<void (size_t id, Error)>;

        /**
         * @brief Creates a pool with a worker count based on hardware_concurrency().
         * @param crushCallback Called if a dispatcher fails; a null callback
         *                     should not be used if dispatcher failure is possible.
         * @return Owning executor, or invalid_worker_count, allocation_failure,
         *         or a worker-creation error (including hardware_concurrency == 0).
         */
        [[nodiscard]] static Result<MiUniquePtr<ThreadPoolExecutor>> create(
            WorkerCrashCallback&& crushCallback = nullptr) noexcept {
            return create(std::thread::hardware_concurrency(), std::move(crushCallback));
        }

        /**
         * @brief Creates a pool of the specified size and starts its workers.
         * @param numWorkers Number of workers; zero yields invalid_worker_count.
         * @param crushCallback Handler for worker dispatcher errors.
         * @return Owning executor, or allocation_failure, invalid_worker_count,
         *         or worker_start_failed.
         */
        [[nodiscard]] static Result<MiUniquePtr<ThreadPoolExecutor>> create(
            size_t numWorkers,
            WorkerCrashCallback&& crushCallback) noexcept;

        /// Stops the pool and waits for all owned jthreads to finish.
        ~ThreadPoolExecutor() override;

        /**
         * @brief Enqueues a coroutine handle in the pool.
         * @param coro Valid handle; the caller retains ownership of the coroutine frame.
         * @return Error for an empty handle, a stopped pool, or a queue rejection.
         * @note Thread safety: supports calls from outside the pool and from its workers.
         */
        [[nodiscard]] Status execute(std::coroutine_handle<> coro) override;

        /**
         * @brief Explicitly submits a handle to the shared incoming queue.
         * @param handle Coroutine handle; ownership of the frame is not transferred.
         * @return queue_enqueue_failed if the queue rejects the task.
         */
        [[nodiscard]] Status enqueueIncoming(std::coroutine_handle<> handle) noexcept;

        /// Creates an uninitialized wrapper; use the create() factory.
        explicit ThreadPoolExecutor() = default;

        /**
         * @brief Constructs an executor from pre-initialized resources.
         *
         * Intended for the factory and low-level use: threads are not started
         * until init(). All supplied containers are moved.
         * @param numWorkers Number of worker contexts.
         * @param threads Vector of threads transferred to the executor.
         * @param workers Vector of worker contexts transferred to the executor.
         * @param incomingQueue Prepared shared queue.
         * @param callback Worker error handler.
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

        /// Starts worker threads after resources have been prepared.
        [[nodiscard]] Status init() noexcept;
        /// Notifies one waiting worker, if any.
        void notifyWorkAvailable() noexcept;

        /// Current WorkerContext for this OS thread; used by execute()'s fast path.
        inline static thread_local WorkerContext* m_currentWorker{ nullptr };

        /// Entry point for the owned worker-thread loop.
        void workerLoop(size_t id) noexcept;

        /// Scheduling dispatcher coroutine for one worker.
        DispatcherTask runDispatcher(WorkerContext *ctx);

        /// MPMC queue for external tasks and local-deque overflow.
        moodycamel::ConcurrentQueue<std::coroutine_handle<>> m_incomingQueue;
        /// Approximate count of incoming items before dequeue.
        alignas(64) std::atomic<uint64_t> m_incomingCount{ 0 };
        /// Number of workers preparing to wait or already waiting for work.
        alignas(64) std::atomic<uint64_t> m_sleepingWorkerCount{ 0 };

        /// Generation-based notification for sleeping dispatchers.
        Notifier m_notifier{};

        /// Owned jthreads; destruction stops and joins them.
        std::vector<std::jthread, MiAllocator<std::jthread>> m_threads;

        /// Worker contexts remain at stable addresses for the lifetime of their threads.
        std::vector<WorkerContext, MiAllocator<WorkerContext>> m_workers;

        /// Once set, blocks new execute() calls; workers stop on this flag.
        alignas(64) std::atomic<bool> m_stopping{ false };
        /// Starting point for choosing the next worker/victim.
        std::atomic<size_t> m_nextWorkerIndex{ 0 };
        /// Number of workers owned by the pool.
        size_t m_numWorkers{0};
        /// Dispatcher error callback moved into the executor.
        WorkerCrashCallback m_crashCallback{ nullptr };
    };
}

#endif //SHUTTLEENGINE_THREADPOOLEXECUTOR_HPP
