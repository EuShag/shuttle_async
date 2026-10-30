//
// Created by Shagu on 22.10.2026.
//

#ifndef SHUTTLE_ASYNC_ASYNCIOTOKEN_HPP
#define SHUTTLE_ASYNC_ASYNCIOTOKEN_HPP

#include <exception>
#include <Windows.h>
#include "async/async_io.hpp"

namespace shuttle::async::threadpool {

    class AsyncIoToken final : public IAsyncIoToken {
    public:
        [[nodiscard]] static std::expected<AsyncIo, RuntimeResultCode> startRead(
            HANDLE hFile, PTP_IO ptpIo, void *dstBuffer, size_t dstBufferSize, size_t fileOffset,
            IntrusiveStrongRef<IAsyncFileToken> &&ref
        );


        [[nodiscard]] static std::expected<AsyncIo, RuntimeResultCode> startWrite(
            HANDLE hFile, PTP_IO ptpIo, const void *srcBuffer, size_t srcBufferSize, size_t fileOffset,
            IntrusiveStrongRef<IAsyncFileToken> &&ref
        );

        void cancel() override;

        static void CALLBACK Win32IoCompletionCallback(
            PTP_CALLBACK_INSTANCE instance,
            PVOID,
            PVOID overlapped,
            ULONG ioResult,
            ULONG_PTR bytes,
            PTP_IO
        );

    private:
        OVERLAPPED m_overlapped{};
        HANDLE m_hFile{};
        AsyncPromise<IoStatus> m_ioPromise;
        IntrusivePtr<IAsyncFileToken>::StrongRef&& m_strongRef ;

        ~AsyncIoToken() override = default;

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

        AsyncIoToken(OVERLAPPED const &overlapped, HANDLE hFile, AsyncPromise<IoStatus>&& promise, IntrusiveStrongRef<IAsyncFileToken> strongRef)
            : m_overlapped{overlapped}, m_hFile{hFile}, m_ioPromise{std::move(promise)}, m_strongRef{std::move(strongRef)} {}
    };
}

#endif //SHUTTLE_ASYNC_ASYNCIOTOKEN_HPP