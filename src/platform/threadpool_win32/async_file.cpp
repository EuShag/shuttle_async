//
// Created by Shagu on 30.10.2026.
//
#include "async/platform/threadpool_win32/async_file.hpp"

#include "async/platform/threadpool_win32/async_io_token.hpp"
#include "async/platform/threadpool_win32/utils.hpp"

namespace shuttle::async::threadpool {
    [[nodiscard]] std::expected<AsyncFile, RuntimeResultCode> AsyncFileToken::create(
            std::filesystem::path const& filePath, DWORD access, DWORD shareMode, DWORD creationDisposition, DWORD flagsAndAttributes, PTP_CALLBACK_ENVIRON environment
        ) noexcept {

        // 1. Создаем локальные RAII-объекты
        HANDLE hFile = ::CreateFileW(filePath.c_str(), access, shareMode, nullptr, creationDisposition, flagsAndAttributes, nullptr);
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
            return std::unexpected{ RuntimeResultCode::allocation_failure };
        }

        return AsyncFile(IntrusivePtr<IAsyncFileToken>{rawToken});
    }

    std::expected<AsyncIo, RuntimeResultCode> AsyncFileToken::read(void *buff, size_t size, size_t offset,
        IntrusiveStrongRef<IAsyncFileToken> &&ref) {
        return AsyncIoToken::startRead(m_hFile.get(), m_ioPtr.get(), buff, size, offset, std::move(ref));
    }

    std::expected<AsyncIo, RuntimeResultCode> AsyncFileToken::write(void const *buff, size_t size, size_t offset,
        IntrusiveStrongRef<IAsyncFileToken> &&ref) {
        return AsyncIoToken::startWrite(m_hFile.get(), m_ioPtr.get(), buff, size, offset, std::move(ref));
    }
}
