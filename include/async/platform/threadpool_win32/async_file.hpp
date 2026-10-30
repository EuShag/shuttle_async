//
// Created by Shagu on 22.10.2026.
//

#ifndef SHUTTLE_ASYNC_ASYNC_FILE_HPP
#define SHUTTLE_ASYNC_ASYNC_FILE_HPP
#include <filesystem>
#include <Windows.h>
#include "async/async_io.hpp"
#include "utils.hpp"

namespace shuttle::async::threadpool {
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
        [[nodiscard]] static std::expected<AsyncFile, RuntimeResultCode> AsyncFileToken::create(
            std::filesystem::path const& filePath, DWORD access, DWORD shareMode, DWORD creationDisposition, DWORD flagsAndAttributes, PTP_CALLBACK_ENVIRON environment
        ) noexcept;

        std::expected<AsyncIo, RuntimeResultCode> read(void *buff, size_t size, size_t offset, IntrusiveStrongRef<IAsyncFileToken>&& ref) override;

        std::expected<AsyncIo, RuntimeResultCode> write(void const* buff, size_t size, size_t offset, IntrusiveStrongRef<IAsyncFileToken>&& ref) override;

        AsyncFileToken(RaiiFile hFile, RaiiPtpIo ioPtr) noexcept : m_hFile{std::move(hFile)}, m_ioPtr{std::move(ioPtr)}{}

        ~AsyncFileToken() override = default;
    private:
        RaiiFile m_hFile;
        RaiiPtpIo m_ioPtr;

        // 1. Переопределяем operator new
        void* operator new(size_t size) {
            return allocateBytes(size, 64ULL);
        }

        // 2. Переопределяем operator delete
        void operator delete(void* ptr, size_t) noexcept {
            if (ptr) {
                deallocateBytes(ptr);
            }
        }
    };
}

#endif //SHUTTLE_ASYNC_ASYNC_FILE_HPP
