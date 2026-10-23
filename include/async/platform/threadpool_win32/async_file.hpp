//
// Created by Shagu on 22.10.2026.
//

#ifndef SHUTTLE_ASYNC_ASYNC_FILE_HPP
#define SHUTTLE_ASYNC_ASYNC_FILE_HPP
#include <Windows.h>
#include "async/async_io.hpp"
#include "async_io_token.hpp"

namespace shuttle::async::threadpool {

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
        RaiiFile(RaiiFile&& other) noexcept : m_hFile{other.m_hFile} { other.m_hFile = INVALID_HANDLE_VALUE; }

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
        RaiiPtpIo(RaiiPtpIo&& other) noexcept : m_ioPtr{other.m_ioPtr} { other.m_ioPtr = nullptr; }

        /// @brief Disables the guard when the PTP_IO object is successfully transferred to the owner.
        void dismiss() noexcept { m_ioPtr = nullptr; }
        [[nodiscard]] PTP_IO get() const noexcept { return m_ioPtr; }

    private:
        PTP_IO m_ioPtr{nullptr};
    };


    class AsyncFileToken final : public IAsyncFileToken {
    public:

        /**
         * @brief Factory method to open a file asynchronously using the Windows Thread Pool.
         *
         * This function handles native file creation, binds the file handle to the
         * Windows thread pool I/O subsystem, and securely packs it into a move-only AsyncFile.
         *
         * @param filePath Path to the file to be opened.
         * @param access Desired access (e.g., GENERIC_READ | GENERIC_WRITE).
         * @param shareMode Share mode (e.g., FILE_SHARE_READ).
         * @param creationDisposition Action to take on files that exist or do not exist (e.g., OPEN_EXISTING).
         * @param environment
         *
         * @return std::expected<AsyncFile, IoResultCode> Returns a valid stack wrapper on success,
         *                                                 or a domain error code on failure.
         */
        [[nodiscard]] static std::expected<AsyncFile, IoResultCode> AsyncFileToken::create(
            const wchar_t* filePath, DWORD access, DWORD shareMode, DWORD creationDisposition, PTP_CALLBACK_ENVIRON environment
        ) noexcept {

            // 1. Создаем локальные RAII-объекты
            HANDLE hFile = ::CreateFileW(filePath, access, shareMode, nullptr, creationDisposition, FILE_FLAG_OVERLAPPED, nullptr);
            if (hFile == INVALID_HANDLE_VALUE) {
                return std::unexpected{ mapWin32Error(::GetLastError()) };
            }
            RaiiFile localFile{hFile};

            // Привязываем колбэк. Передаем nullptr вместо контекста, так как контекст вытащим из OVERLAPPED
            PTP_IO ptpIo = ::CreateThreadpoolIo(hFile, &AsyncIoToken::Win32IoCompletionCallback, nullptr, environment);
            if (!ptpIo) {
                return std::unexpected{ mapWin32Error(::GetLastError()) }; // localFile сам закроет hFile
            }
            RaiiPtpIo localIo{ptpIo};

            // 2. Выделяем токен и ПЕРЕМЕЩАЕМ в него наши RAII-поля
            auto* rawToken = new AsyncFileToken(std::move(localFile), std::move(localIo));
            if (!rawToken) {
                // Если памяти нет, localIo и localFile (которые не были перемещены в случае исключения,
                // но здесь мы использовали std::move, поэтому они очистятся локально, если rawToken не создался)
                return std::unexpected{ IoResultCode::allocation_failed };
            }

            return AsyncFile(rawToken);
        }



        std::expected<AsyncIo, IoResultCode> read(void *buff, size_t size, size_t offset) override {
            return AsyncIoToken::startRead(m_hFile.get(), m_ioPtr.get(), buff, size, offset, AsyncFileTokenGuard{this});
        }

        std::expected<AsyncIo, IoResultCode> write(void const* buff, size_t size, size_t offset) override {
            return AsyncIoToken::startWrite(m_hFile.get(), m_ioPtr.get(), buff, size, offset, AsyncFileTokenGuard{this});
        }

        AsyncFileToken(RaiiFile hFile, RaiiPtpIo ioPtr) noexcept : m_hFile{std::move(hFile)}, m_ioPtr{std::move(ioPtr)}{}

        // 1. Переопределяем operator new
        void* operator new(size_t size) {
            return MiAllocator<char>().allocate(size);
        }

        // 2. Переопределяем operator delete
        void operator delete(void* ptr, size_t) noexcept {
            if (ptr) {
                MiAllocator<char>().deallocate(static_cast<char*>(ptr));
            }
        }

        void acquire() { m_refCount.fetch_add(1, std::memory_order_relaxed); }
        void release() {
            // fetch_sub уменьшает счетчик и возвращает СТАРОЕ значение.
            // Если старое значение было 1, значит новое стало 0 — мы были ПОСЛЕДНИМ владельцем.
            if (m_refCount.fetch_sub(1, std::memory_order_release) == 1) {

                // Барьер памяти (fence) гарантирует, что кэш процессора синхронизирован,
                // и все фоновые потоки завершили запись данных в этот объект.
                std::atomic_thread_fence(std::memory_order_acquire);

                // ОБЪЕКТ УНИЧТОЖАЕТ САМ СЕБЯ
                delete this;
            }
        }
    private:
        RaiiFile m_hFile;
        RaiiPtpIo m_ioPtr;
        std::atomic<uint64_t> m_refCount{0};
    };
}

#endif //SHUTTLE_ASYNC_ASYNC_FILE_HPP
