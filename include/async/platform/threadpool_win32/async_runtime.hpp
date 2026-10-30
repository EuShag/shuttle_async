//
// Created by Shagu on 27.10.2026.
//

#ifndef SHUTTLE_ASYNC_ASYNC_RUNTIME_HPP
#define SHUTTLE_ASYNC_ASYNC_RUNTIME_HPP
#include "concurrentqueue.h"
#include <Windows.h>

#include "async_file.hpp"
#include "async/async_runtime.hpp"

namespace shuttle::async::threadpool {




    /**
     * @struct Win32FileArguments
     * @brief Результат трансляции FileOptions в системные флаги Win32.
     */
    struct Win32FileArguments {
        DWORD desired_access;
        DWORD share_mode;
        DWORD creation_disposition;
        DWORD flags_and_attributes;
    };

    /**
     * @brief Транслирует кроссплатформенные FileOptions в нативные аргументы Win32 CreateFile.
     */
    constexpr Win32FileArguments translateFileOptionsToWin32(const FileOptions& options);

    class RaiiPtpCleanGroup {
    public:
        RaiiPtpCleanGroup(PTP_CLEANUP_GROUP ptpCleanGroup) : m_ptpCleanGroup{ptpCleanGroup} {}

        RaiiPtpCleanGroup(RaiiPtpCleanGroup const&) = delete;
        RaiiPtpCleanGroup& operator=(RaiiPtpCleanGroup const&) = delete;

        RaiiPtpCleanGroup(RaiiPtpCleanGroup&& other)  noexcept : m_ptpCleanGroup{std::exchange(other.m_ptpCleanGroup, nullptr)} {}
        RaiiPtpCleanGroup& operator=(RaiiPtpCleanGroup&& other) noexcept {
            if (this == &other) return *this;
            if (m_ptpCleanGroup) {
                CloseThreadpoolCleanupGroup(m_ptpCleanGroup);
            }
            m_ptpCleanGroup = std::exchange(other.m_ptpCleanGroup, nullptr);
            return *this;
        }

        [[nodiscard]] PTP_CLEANUP_GROUP get() const { return m_ptpCleanGroup; }

        ~RaiiPtpCleanGroup() {
            CloseThreadpoolCleanupGroup(m_ptpCleanGroup);
        }

    private:
        PTP_CLEANUP_GROUP m_ptpCleanGroup;
    };

    class AsyncRuntime;

    // Выделяется в timerBuff
    class TimerWaitToken{};
    // Выделяется в waitBuff
    class EventWaitToken{};

    // Выделяется в куче и живёт до уничтожения таймера
    class TimerWaitSlot {
        PTP_TIMER m_ptpTimer{};
        AsyncRuntime* m_pAsyncRuntime{};
        char m_timerBuff[sizeof(TimerWaitToken)]{};
    };

    class EventWaitSlot {
        PTP_WAIT m_ptpWait{};
        HANDLE m_hEvent{};
        AsyncRuntime* m_pAsyncRuntime{};
        char m_waitBuff[sizeof(EventWaitToken)]{};
    };

    class AsyncRuntime final : public IAsyncRuntime {
    public:

        void releaseTimer(TimerWaitSlot* timer) {
            m_timerWaitSlotPool.enqueue(timer);
        }

        void releaseEvent(EventWaitSlot* event) {
            m_eventWaitSlotPool.enqueue(event);
        }

        void closeFile(AsyncFile* file) {
        }

        [[nodiscard]] static Result<IAsyncRuntime*> create() {

            auto ptpPool = CreateThreadpool(nullptr);
            auto ptpCleanGroup = CreateThreadpoolCleanupGroup();

            SetThreadpoolThreadMinimum(ptpPool, 4);
            SetThreadpoolThreadMaximum(ptpPool, 64);

            auto* asyncRuntimePtr = new AsyncRuntime({ptpPool}, {ptpCleanGroup});
            if (asyncRuntimePtr) {
                return std::unexpected{Error::allocation_failure};
            }
            return asyncRuntimePtr;
        }

    private:

        RaiiPtpPool m_ptpPool;
        RaiiPtpCleanGroup m_ptpCleanGroup;
        RaiiTpCallbackEnviron m_callbackEnviron;

        // Пулы для свободных таймеров и событий
        moodycamel::ConcurrentQueue<TimerWaitSlot*> m_timerWaitSlotPool{};
        moodycamel::ConcurrentQueue<EventWaitSlot*> m_eventWaitSlotPool{};

        AsyncRuntime(RaiiPtpPool&& ptpPool, RaiiPtpCleanGroup&& ptpCleanGroup) :
            m_ptpPool{std::move(ptpPool)}, m_ptpCleanGroup{std::move(ptpCleanGroup)}, m_callbackEnviron{ptpPool.get(), ptpCleanGroup.get()} {}

        ~AsyncRuntime() override {
            // Зачищаем спящие таймеры
            TimerWaitSlot* idleTimer = nullptr;
            while (m_timerWaitSlotPool.try_dequeue(idleTimer)) {
                delete idleTimer;
            }

            // Зачищаем спящие события
            EventWaitSlot* idleEvent = nullptr;
            while (m_eventWaitSlotPool.try_dequeue(idleEvent)) {
                delete idleEvent;
            }
        }

        void* operator new(size_t size) {
            return allocateBytes(size, 64UL);
        }
        void operator delete(void* ptr, size_t) {
            deallocateBytes(ptr);
        }

    public:
        std::expected<AsyncFile, RuntimeResultCode> createFile(std::filesystem::path const& path, FileOptions options, IntrusiveStrongRef<IAsyncRuntime>&& ref) override;

        void delay(std::chrono::milliseconds ms, IntrusiveStrongRef<IAsyncRuntime>&& ref) override;

        void setTimeout(
            ICancellable* cancellable,
            std::chrono::milliseconds ms, IntrusiveStrongRef<IAsyncRuntime>&& ref) override;

        IEvent* getEvent(IntrusiveStrongRef<IAsyncRuntime>&& ref) override;

        // Подписка на системный хэндл Windows (Event, Semaphore, Mutex) с ограничением по времени
        void waitEvent(IEvent* hEvent, std::chrono::milliseconds timeoutMs, IntrusiveStrongRef<IAsyncRuntime>&& ref) override; // здесь тоже
    };

}

#endif //SHUTTLE_ASYNC_ASYNC_RUNTIME_HPP
