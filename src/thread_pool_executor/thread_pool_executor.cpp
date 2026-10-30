//
// Created by Shagu on 13.10.2026.
//

#include <async/thread_pool_executor/thread_pool_executor.hpp>
#include <async/try.hpp>

#include <xmmintrin.h>

namespace shuttle::async {
    Status WorkerContext::WorkerExecutor::execute(std::coroutine_handle<> coro) noexcept {
        workerContext->m_serviceQueueSize.fetch_add(1, std::memory_order_relaxed);
        if (!workerContext->m_serviceQueue.try_enqueue(coro)) {
            workerContext->m_serviceQueueSize.fetch_sub(1, std::memory_order_relaxed);
            return std::unexpected(Error::queue_enqueue_failed);
        }
        wakeUpWorker();
        return {};
    }

    void WorkerContext::WorkerExecutor::wakeUpWorker() const noexcept {
        if (workerContext && threadPoolExecutor) [[likely]] {
            threadPoolExecutor->m_notifier.notify_all();
        }
    }

        [[nodiscard]] Result<MiUniquePtr<ThreadPoolExecutor>> ThreadPoolExecutor::create(
        size_t numWorkers,
        WorkerCrashCallback&& crushCallback) noexcept
    {
        if (numWorkers == 0) {
            return std::unexpected(Error::invalid_worker_count);
        }

        // 1. Declare transient containers on the factory stack frame.
        std::optional<moodycamel::ConcurrentQueue<std::coroutine_handle<>>> localQueueOpt;
        std::optional<std::vector<WorkerContext, MiAllocator<WorkerContext>>> localContextsOpt;
        std::vector<std::jthread, MiAllocator<std::jthread>> localThreads;

        // 2. EXCEPTION SAFETY BLOCK
        try {
            localQueueOpt.emplace();
            localThreads.reserve(numWorkers);
            localContextsOpt.emplace(numWorkers);
        }
        catch (...) {
            return std::unexpected(Error::allocation_failure);
        }

        auto& localContexts = *localContextsOpt;
        auto& localQueue = *localQueueOpt;

        // Configure IDs and reset error atomics within the pre-allocated monolithic memory block.
        for (size_t i = 0; i < numWorkers; ++i) {
            localContexts[i].m_id = i;
        }

        // 3. Assemble the executor wrapper object.
        // Ownership of the containers is permanently moved into the smart pointer fields.
        auto executor = mi_make_unique_nothrow<ThreadPoolExecutor>(
            numWorkers,
            std::move(localThreads),
            std::move(localContexts),
            std::move(localQueue),
            std::move(crushCallback)
        );

        if (!executor) [[unlikely]] {
            return std::unexpected(Error::allocation_failure);
        }

        // 4. POOL INITIALIZATION (The critical startup phase)
        // Now that the executor is fully assembled and allocated in the heap via mi_make_unique_nothrow,
        // its memory address is GUARANTEED TO BE STABLE. The pointer returned by executor.get() will never change!
        // We can safely invoke init(), ensuring that the OS threads receive a permanent, valid 'this' address.
        auto initResult = executor->init();
        if (!initResult) [[unlikely]] {
            return std::unexpected(initResult.error());
        }

        // 5. SUCCESS: Return a live, running, robust thread pool instance!
        return std::move(executor);
    }


    ThreadPoolExecutor::~ThreadPoolExecutor() {
        m_stopping.store(true, std::memory_order_release);
        m_notifier.notify_all();
        m_threads.clear();
    }

        Status ThreadPoolExecutor::execute(
        std::coroutine_handle<> coro) {
        // 1. STRICT INPUT BARRIER
        if (!coro) [[unlikely]] {
            return std::unexpected(Error::invalid_coroutine_handle);
        }

        // A hard barrier during pool Shutdown: no new tasks are accepted.
        // This causes the call to bounce back to the Yield or launch context, allowing coroutines to cleanly release RAII resources.
        if (m_stopping.load(std::memory_order_acquire)) [[unlikely]] {
            return std::unexpected(Error::executor_stopped);
        }

        // 2. DETERMINE THE CALL CONTEXT
        // Extract the TLS pointer of the current physical OS thread
        WorkerContext* currentCtx = m_currentWorker;

        if (currentCtx != nullptr) {
            // =================================================================
            // FAST-PATH (Internal Pool Call): A coroutine spawns a coroutine.
            // =================================================================
            // Push the task DIRECTLY into the local Chase-Lev deque without any locking overhead
            if (!currentCtx->m_queue.try_push(coro)) [[unlikely]] {

                // LOCAL BUFFER OVERFLOW (>1024 tasks):
                // Instead of crashing, we gracefully offload the excess tasks into the global incoming queue.
                // Use our void-group macro, which returns an error if the system runs out of memory!
                ASYNC_TRY_VOID(enqueueIncoming(coro));
                return {};
            }

            notifyWorkAvailable();
        }
        else {
            // =================================================================
            // SLOW-PATH (External Thread Call): Invoked from main(), network I/O, or OS callback.
            // =================================================================
            // Safely and reliably enqueue the root task into the global lock-free buffer
            ASYNC_TRY_VOID(enqueueIncoming(coro));
        }

        return {};
    }

    // =================================================================
    // HELPER METHOD FOR INTERACTIONS WITH THE GLOBAL TASK STORAGE
    // =================================================================
    Status ThreadPoolExecutor::enqueueIncoming(std::coroutine_handle<> handle) noexcept {
        m_incomingCount.fetch_add(1, std::memory_order_release);

        // moodycamel will request new memory pages from the OS if the existing ones are full.
        // If the operating system is completely out of memory, the method will return false.
        if (!m_incomingQueue.enqueue(handle)) [[unlikely]] {
            m_incomingCount.fetch_sub(1, std::memory_order_relaxed);
            // The only legitimate technical internal error of the worker/pool:
            return std::unexpected(Error::queue_enqueue_failed);
        }

        // Wake up a single pool worker to process the task from the global storage
        notifyWorkAvailable();
        return {};
    }


    void ThreadPoolExecutor::notifyWorkAvailable() noexcept {
        if (m_sleepingWorkerCount.load(std::memory_order_acquire) != 0) {
            m_notifier.notify_one();
        }
    }

    Status ThreadPoolExecutor::init() noexcept {
        if (!m_threads.empty()) [[unlikely]] {
            return {};
        }

        for (size_t i = 0; i < m_numWorkers; ++i) {
            try {
                // Launch jthread. Pass the loop method, the pool address, and the worker index.
                m_threads.emplace_back(&ThreadPoolExecutor::workerLoop, this, i);
            }
            catch (const std::system_error&) {
                // 🚨 CRITICAL OS FAILURE: Process descriptor or thread limits exceeded!
                m_stopping.store(true, std::memory_order_release);
                m_notifier.notify_all(); // Wake up any workers that managed to fall asleep.

                // The jthread destructors will automatically join() all successfully launched threads.
                m_threads.clear();

                return std::unexpected(Error::worker_start_failed);
            }
        }

        return {};
    }

    void ThreadPoolExecutor::workerLoop(size_t id) noexcept {
        WorkerContext& ctx = m_workers[id];
        ctx.m_serviceQueueConsumerToken.emplace(ctx.m_serviceQueue);
        ctx.m_incomingQueueConsumerToken.emplace(m_incomingQueue);

        // Инициализируем вшитый контекст продолжения прямо перед пуском
        ctx.m_continuation = WorkerContext::WorkerExecutor{&ctx, this};
        ctx.m_randomState = id + 1 | 1u;

        auto dispatcher = runDispatcher(&ctx);
        ctx.m_dispatcher = dispatcher;

        if (!dispatcher.has_value()) [[unlikely]] { m_crashCallback(ctx.m_id, dispatcher.error()); return; }
        ThreadContextGuard currentExecutorGuard({
            .m_threadPinnedExecutor = &ctx.m_continuation,
            .m_dispatcherCoroHandle = dispatcher
        });

        m_currentWorker = &ctx;
        ctx.m_dispatcher.resume();
        m_currentWorker = nullptr;
    }

    DispatcherTask ThreadPoolExecutor::runDispatcher(WorkerContext* ctx) {
        // =================================================================
        // ОСНОВНОЙ СИНХРОННЫЙ ЦИКЛ ПЛАНИРОВАНИЯ ТЕКУЩЕГО ПОТОКА
        // =================================================================
        uint32_t spin = 0;
        while (true) {
            // КРИТИЧЕСКИЙ ИНВАРИАНТ ОСТАНОВКИ ПУЛА (При выходе из игры)
            if (m_stopping.load(std::memory_order_relaxed)) [[unlikely]] {
                if (ctx->m_queue.empty()
                    && ctx->m_serviceQueueSize.load(std::memory_order_relaxed) == 0
                    && m_incomingCount.load(std::memory_order_relaxed) == 0) {
                    break; // Мягко тушим поток ОС Windows
                }
            }

            std::coroutine_handle targetHandle = nullptr;
            // ШАГ 0: Пакетная вычистка Шага 0 (Приоритетная служебная очередь)
            if (ctx->m_serviceQueue.try_dequeue(*ctx->m_serviceQueueConsumerToken, targetHandle) && targetHandle) {
                ctx->m_serviceQueueSize.fetch_sub(1, std::memory_order_acq_rel);
                spin = 0;
                co_yield targetHandle;
                continue;
            }

            // ШАГ 1: Локальная дека Chase-Lev (Молниеносный LIFO воркера)
            if (auto handle = ctx->m_queue.pop()) {
                spin = 0;
                co_yield handle;
                continue;
            }

            // ШАГ 2: Входящая глобальная воронка пула задач (FIFO общего склада)
            uint64_t const incomingCount = m_incomingCount.load(std::memory_order_acquire);
            if (incomingCount > 0) {
                std::coroutine_handle globalCoro = nullptr;
                if (m_incomingQueue.try_dequeue(*ctx->m_incomingQueueConsumerToken, globalCoro) && globalCoro) {
                    m_incomingCount.fetch_sub(1, std::memory_order_relaxed);
                    spin = 0;
                    co_yield globalCoro;
                    continue;
                }
            }

            // ШАГ 3: Рандомизированное Work-Stealing у коллег по пулу (Xorshift FIFO с головы)
            ctx->m_randomState ^= ctx->m_randomState << 13;
            ctx->m_randomState ^= ctx->m_randomState >> 17;
            ctx->m_randomState ^= ctx->m_randomState << 5;

            size_t const startVictimId = ctx->m_randomState % m_numWorkers;

            std::coroutine_handle stealCoroHandle = nullptr;
            for (size_t i = 0; i < m_numWorkers; ++i) {
                size_t const victimId = (startVictimId + i) % m_numWorkers;

                if (victimId != ctx->m_id) {
                    if (auto handle = m_workers[victimId].m_queue.steal()) {
                        stealCoroHandle = handle;
                        break;
                    }
                }
            }
            if (stealCoroHandle != nullptr) {
                spin = 0;
                co_yield stealCoroHandle;
                continue;
            }

            // 1. АКТИВНЫЙ SPIN-WAIT (500 микро-кругов микро-паузы процессора)
            if (spin < 500) {
                #if defined(_MSC_VER) || defined(__x86_64__) || defined(__i386__)
                _mm_pause(); // Спасаем ядро CPU от теплового перегрева
                 #endif
                spin++;
            }
            else {
                // 2. ТЯЖЕЛЫЙ СТАТИЧЕСКИЙ СОН НА ВАШЕМ GENERATION NOTIFIER
                // Поток ОС Windows безболезненно засыпает в анабиозе ядра ОС через WaitOnAddress
                m_sleepingWorkerCount.fetch_add(1, std::memory_order_acq_rel);
                m_notifier.wait([this, &ctx]() noexcept {
                    if (m_stopping.load(std::memory_order_acquire)) [[unlikely]] return true;

                    // Проверяем точные лок-фри счетчики и деки без лишнего оверхеда
                    if (ctx->m_serviceQueueSize.load(std::memory_order_acquire) > 0) return true;
                    if (!ctx->m_queue.empty()) return true;
                    if (m_incomingCount.load(std::memory_order_acquire) > 0) return true;
                    for (size_t workerId = 0; workerId < m_numWorkers; ++workerId) {
                        if (workerId != ctx->m_id && !m_workers[workerId].m_queue.empty()) return true;
                    }

                    return false; // Всё пусто — продолжаем спать в ядре ОС
                });
                m_sleepingWorkerCount.fetch_sub(1, std::memory_order_acq_rel);
                spin = 0;
            }
        }
        co_return;
    }

}
