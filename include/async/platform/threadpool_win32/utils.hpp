//
// Created by Shagu on 30.10.2026.
//

#ifndef SHUTTLE_ASYNC_UTILS_HPP
#define SHUTTLE_ASYNC_UTILS_HPP
#include <utility>

#include "async/runtime_result_code.hpp"
#include <Windows.h>


#define CONST_CONTAINING_RECORD(address, type, field) \
    reinterpret_cast<type*>(                          \
        reinterpret_cast<std::uintptr_t>(address) - offsetof(type, field) \
    )

namespace shuttle::async::threadpool {
    [[nodiscard]] constexpr RuntimeResultCode mapWin32Error(DWORD win32Error) noexcept;

    /**
     * @brief Scope guard for safe Windows HANDLE cleanup during initialization.
     */
    class RaiiFile {
    public:
        explicit RaiiFile(HANDLE hFile) noexcept : m_hFile{hFile} {}

        ~RaiiFile() {
            if (m_hFile && m_hFile != INVALID_HANDLE_VALUE) {
                ::CloseHandle(m_hFile);
            }
        }

        // Move-only semantics
        RaiiFile(const RaiiFile&) = delete;
        RaiiFile& operator=(const RaiiFile&) = delete;
        RaiiFile(RaiiFile&& other) noexcept : m_hFile{std::exchange(other.m_hFile, INVALID_HANDLE_VALUE)} { }

        /// @brief Disables the guard when the handle is successfully transferred to the owner.
        void dismiss() noexcept { m_hFile = INVALID_HANDLE_VALUE; }
        [[nodiscard]] HANDLE get() const noexcept { return m_hFile; }

    private:
        HANDLE m_hFile{INVALID_HANDLE_VALUE};
    };

    /**
     * @brief Scope guard for safe PTP_IO cleanup during initialization.
     */
    class RaiiPtpIo {
    public:
        explicit RaiiPtpIo(PTP_IO ioPtr) noexcept : m_ioPtr{ioPtr} {}

        ~RaiiPtpIo() {
            if (m_ioPtr) {
                ::CloseThreadpoolIo(m_ioPtr);
            }
        }

        // Move-only semantics
        RaiiPtpIo(const RaiiPtpIo&) = delete;
        RaiiPtpIo& operator=(const RaiiPtpIo&) = delete;
        RaiiPtpIo(RaiiPtpIo&& other) noexcept : m_ioPtr{std::exchange(other.m_ioPtr, nullptr)} { }

        /// @brief Disables the guard when the PTP_IO object is successfully transferred to the owner.
        void dismiss() noexcept { m_ioPtr = nullptr; }
        [[nodiscard]] PTP_IO get() const noexcept { return m_ioPtr; }

    private:
        PTP_IO m_ioPtr{nullptr};
    };

    class RaiiPtpPool {
    public:
        RaiiPtpPool(PTP_POOL pool) : m_pool{pool} {}

        RaiiPtpPool(RaiiPtpPool const&) = delete;
        RaiiPtpPool& operator=(RaiiPtpPool const&) = delete;

        RaiiPtpPool(RaiiPtpPool&& other)  noexcept : m_pool{std::exchange(other.m_pool, nullptr)} {}
        RaiiPtpPool& operator=(RaiiPtpPool&& other) noexcept {
            if (this == &other) return *this;
            if (m_pool) {
                CloseThreadpool(m_pool);
            }
            m_pool = std::exchange(other.m_pool, nullptr);
            return *this;
        }

        [[nodiscard]] PTP_POOL get() const { return m_pool; }

        ~RaiiPtpPool() {
            CloseThreadpool(m_pool);
        }
    private:
        PTP_POOL m_pool{};
    };

    class RaiiTpCallbackEnviron {
    public:
        RaiiTpCallbackEnviron(PTP_POOL pool, PTP_CLEANUP_GROUP cleanupGroup) {
            InitializeThreadpoolEnvironment(&m_environ);
            SetThreadpoolCallbackPool(&m_environ, pool);
            SetThreadpoolCallbackCleanupGroup(&m_environ, cleanupGroup, nullptr);
        }

        RaiiTpCallbackEnviron(RaiiTpCallbackEnviron const&) = delete;
        RaiiTpCallbackEnviron& operator=(RaiiTpCallbackEnviron const&) = delete;

        [[nodiscard]] PTP_CALLBACK_ENVIRON get() { return &m_environ; }

        ~RaiiTpCallbackEnviron() {
            DestroyThreadpoolEnvironment(&m_environ);
        }
    private:
        TP_CALLBACK_ENVIRON m_environ{};
    };
}

#endif //SHUTTLE_ASYNC_UTILS_HPP
