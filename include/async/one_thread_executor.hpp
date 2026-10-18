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
     * @if RU
     * @brief Исполнитель с одним worker-потоком и очередью coroutine handles.
     *
     * create() создаёт экземпляр и запускает worker. execute() ставит задачи
     * в очередь; объект не копируется и не перемещается. Разрушение инициирует
     * остановку и присоединяет worker, поэтому executor должен жить дольше
     * всех coroutine, которые могут его использовать. Разрушать executor
     * непосредственно на его worker-потоке нельзя: реализация вызывает terminate.
     * @endif
     * @if EN
     * @brief Executor with a single worker thread and a queue of coroutine handles.
     *
     * `create()` creates an instance and starts its worker. `execute()` enqueues
     * tasks; the object is neither copyable nor movable. Destruction initiates
     * shutdown and joins the worker, so the executor must outlive every coroutine
     * that may use it. Destroying the executor on its own worker thread is
     * forbidden: the implementation calls `terminate`.
     * @endif
     */
    class OneThreadExecutor final : public IExecutor {
    public:
        /// @if RU
        /// Создаёт executor без запуска; для фактической работы вызовите start().
        /// @endif
        /// @if EN
        /// Creates an executor without starting it; call start() to run it.
        /// @endif
        OneThreadExecutor() noexcept = default;
        /// @if RU
        /// Копирование запрещено: объект содержит worker и синхронизацию.
        /// @endif
        /// @if EN
        /// Copying is disabled because the object contains a worker and synchronization state.
        /// @endif
        OneThreadExecutor(const OneThreadExecutor&) = delete;
        OneThreadExecutor& operator=(const OneThreadExecutor&) = delete;
        /// @if RU
        /// Перемещение запрещено: worker хранит адрес executor-а.
        /// @endif
        /// @if EN
        /// Moving is disabled because the worker stores the executor's address.
        /// @endif
        OneThreadExecutor(OneThreadExecutor&&) = delete;
        OneThreadExecutor& operator=(OneThreadExecutor&&) = delete;
        /// @if RU
        /// Останавливает worker и ожидает его завершения.
        /// @endif
        /// @if EN
        /// Stops the worker and waits for it to finish.
        /// @endif
        ~OneThreadExecutor() override;

        /**
         * @if RU
         * @brief Выделяет и запускает однопоточный executor.
         * @return Уникальный владеющий указатель либо allocation_failure /
         *         worker_start_failed.
         * @endif
         * @if EN
         * @brief Allocates and starts a single-threaded executor.
         * @return Unique owning pointer, or allocation_failure /
         *         worker_start_failed.
         * @endif
         */
        [[nodiscard]] static Result<MiUniquePtr<OneThreadExecutor>> create() noexcept;

        /**
         * @if RU
         * @brief Идемпотентно запускает worker.
         * @return Успех либо ошибка создания worker-а.
         * @note Thread safety: параллельные вызовы сериализованы внутренним состоянием запуска.
         * @endif
         * @if EN
         * @brief Starts the worker idempotently.
         * @return Success or a worker-creation error.
         * @note Thread safety: concurrent calls are serialized by the internal start state.
         * @endif
         */
        [[nodiscard]] Status start() noexcept;
        /**
         * @if RU
         * @brief Ставит корутину в очередь worker-а.
         * @param coroutine Валидный handle frame, lifetime которого остаётся у вызывающего.
         * @return Ошибка для пустого handle, неготового/остановленного executor-а
         *         либо отказа очереди.
         * @endif
         * @if EN
         * @brief Enqueues a coroutine on the worker's queue.
         * @param coroutine Valid frame handle whose lifetime remains the caller's responsibility.
         * @return Error for an empty handle, an unready/stopped executor, or a queue rejection.
         * @endif
         */
        [[nodiscard]] Status execute(std::coroutine_handle<> coroutine) override;

    private:
        static constexpr uint64_t STOPPING_BIT = uint64_t{1} << 63;
        static constexpr uint64_t ACTIVE_SUBMISSIONS_MASK = ~STOPPING_BIT;
        static constexpr int START_NOT_STARTED = 0;
        static constexpr int START_IN_PROGRESS = 1;
        static constexpr int START_READY = 2;
        static constexpr int START_FAILED = 3;

        /// @if RU
        /// Цикл единственного worker-а; завершает dispatcher при остановке и пустой очереди.
        /// @endif
        /// @if EN
        /// Single-worker loop; finishes the dispatcher when stopping with an empty queue.
        /// @endif
        void workerLoop() noexcept;
        /// @if RU
        /// Генерирует dispatcher, извлекающий задачи из очереди и ожидающий уведомления.
        /// @endif
        /// @if EN
        /// Coroutine dispatcher that dequeues tasks and waits for notifications.
        /// @endif
        DispatcherTask runDispatcher();

        /// @if RU
        /// Очередь переданных coroutine handle-ов; frame принадлежат отправителям.
        /// @endif
        /// @if EN
        /// Queue of submitted coroutine handles; frames remain owned by submitters.
        /// @endif
        moodycamel::ConcurrentQueue<std::coroutine_handle<>> m_queue;
        /// @if RU
        /// Consumer token, создаваемый и используемый только worker-потоком.
        /// @endif
        /// @if EN
        /// Consumer token created and used only by the worker thread.
        /// @endif
        std::optional<moodycamel::ConsumerToken> m_queueConsumerToken;
        /// @if RU
        /// Число поставленных и ещё не извлечённых задач.
        /// @endif
        /// @if EN
        /// Number of enqueued tasks not yet dequeued.
        /// @endif
        std::atomic<uint64_t> m_pendingCount{0};
        /// @if RU
        /// Старший бит запрещает новые submissions, младшие считают активные отправки.
        /// @endif
        /// @if EN
        /// The high bit blocks new submissions; the low bits count active submissions.
        /// @endif
        std::atomic<uint64_t> m_lifecycle{0};
        /// @if RU
        /// Состояние одноразового запуска: предотвращает несколько worker-ов.
        /// @endif
        /// @if EN
        /// One-time start state; prevents creation of multiple workers.
        /// @endif
        std::atomic<int> m_startState{START_NOT_STARTED};
        /// @if RU
        /// Ошибка, опубликованная при невозможности создать worker/dispatcher.
        /// @endif
        /// @if EN
        /// Error published if the worker/dispatcher cannot be created.
        /// @endif
        std::atomic<Error> m_startError{Error::worker_start_failed};
        /// @if RU
        /// Пробуждает dispatcher при новой работе или остановке.
        /// @endif
        /// @if EN
        /// Wakes the dispatcher when new work arrives or shutdown begins.
        /// @endif
        Notifier m_notifier;
        /// @if RU
        /// Единственный принадлежащий worker; его destructor выполняет join.
        /// @endif
        /// @if EN
        /// The sole owned worker; its destructor joins the thread.
        /// @endif
        std::jthread m_worker;
    };

}
