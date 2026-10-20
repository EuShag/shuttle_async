#pragma once

#include <async/thread_pool_executor/notifier.hpp>
#include <async/i_executor.hpp>
#include <async/mi_allocator.hpp>
#include <async/thread_pool_executor/thread_pool_dispatcher_task.hpp>

#include <concurrentqueue.h>

#include <atomic>
#include <coroutine>
#include <expected>
#include <cstdint>
#include <thread>

namespace shuttle::async {

    /**
     * @brief Executor with a single worker thread and a queue of coroutine handles.
     *
     * `create()` creates an instance and starts its worker. `execute()` enqueues
     * tasks; the object is neither copyable nor movable. Destruction initiates
     * shutdown and joins the worker, so the executor must outlive every coroutine
     * that may use it. Destroying the executor on its own worker thread is
     * forbidden: the implementation calls `terminate`.
     */
    class OneThreadExecutor final : public IExecutor {
    public:
        /// Creates an executor without starting it; call start() to run it.
        OneThreadExecutor() noexcept = default;
        /// Copying is disabled because the object contains a worker and synchronization state.
        OneThreadExecutor(const OneThreadExecutor&) = delete;
        OneThreadExecutor& operator=(const OneThreadExecutor&) = delete;
        /// Moving is disabled because the worker stores the executor's address.
        OneThreadExecutor(OneThreadExecutor&&) = delete;
        OneThreadExecutor& operator=(OneThreadExecutor&&) = delete;
        /// Stops the worker and waits for it to finish.
        ~OneThreadExecutor() override;

        /**
         * @brief Allocates and starts a single-threaded executor.
         * @return Unique owning pointer, or allocation_failure /
         *         worker_start_failed.
         */
        [[nodiscard]] static Result<MiUniquePtr<OneThreadExecutor>> create() noexcept;

        /**
         * @brief Starts the worker idempotently.
         * @return Success or a worker-creation error.
         * @note Thread safety: concurrent calls are serialized by the internal start state.
         */
        [[nodiscard]] Status start() noexcept;
        /**
         * @brief Enqueues a coroutine on the worker's queue.
         * @param coroutine Valid frame handle whose lifetime remains the caller's responsibility.
         * @return Error for an empty handle, an unready/stopped executor, or a queue rejection.
         */
        [[nodiscard]] Status execute(std::coroutine_handle<> coroutine) override;

    private:
        static constexpr uint64_t STOPPING_BIT = uint64_t{1} << 63;
        static constexpr uint64_t ACTIVE_SUBMISSIONS_MASK = ~STOPPING_BIT;
        static constexpr int START_NOT_STARTED = 0;
        static constexpr int START_IN_PROGRESS = 1;
        static constexpr int START_READY = 2;
        static constexpr int START_FAILED = 3;

        /// Single-worker loop; finishes the dispatcher when stopping with an empty queue.
        void workerLoop() noexcept;
        /// Coroutine dispatcher that dequeues tasks and waits for notifications.
        DispatcherTask runDispatcher();

        /// Queue of submitted coroutine handles; frames remain owned by submitters.
        moodycamel::ConcurrentQueue<std::coroutine_handle<>> m_queue;
        /// Consumer token created and used only by the worker thread.
        std::optional<moodycamel::ConsumerToken> m_queueConsumerToken;
        /// Number of enqueued tasks not yet dequeued.
        std::atomic<uint64_t> m_pendingCount{0};
        /// The high bit blocks new submissions; the low bits count active submissions.
        std::atomic<uint64_t> m_lifecycle{0};
        /// One-time start state; prevents creation of multiple workers.
        std::atomic<int> m_startState{START_NOT_STARTED};
        /// Error published if the worker/dispatcher cannot be created.
        std::atomic<Error> m_startError{Error::worker_start_failed};
        /// Wakes the dispatcher when new work arrives or shutdown begins.
        Notifier m_notifier;
        /// The sole owned worker; its destructor joins the thread.
        std::jthread m_worker;
    };

}
