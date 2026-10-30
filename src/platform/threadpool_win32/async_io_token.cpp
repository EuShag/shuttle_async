//
// Created by Shagu on 30.10.2026.
//
#include "async/platform/threadpool_win32/async_io_token.hpp"

#include "async/platform/threadpool_win32/utils.hpp"

namespace shuttle::async::threadpool {
    std::expected<AsyncIo, RuntimeResultCode> AsyncIoToken::startRead(HANDLE hFile, PTP_IO ptpIo, void *dstBuffer,
                                                                      size_t dstBufferSize, size_t fileOffset,
                                                                      IntrusiveStrongRef<IAsyncFileToken> &&ref) {
        // 1. Выделяем SharedState для передачи данных
        auto createStateResult = AsyncStateFactory<IoStatus>::create();
        if (!createStateResult.has_value()) {
            return std::unexpected{RuntimeResultCode::allocation_failure};
        }

        // 2. Выделяем Токен (теперь из нашего быстрого PMR пул-аллокатора благодаря переопределению new)
        auto* token = new AsyncIoToken({
                                           .Offset = static_cast<DWORD>(fileOffset & 0xFFFFFFFF),
                                           .OffsetHigh = static_cast<DWORD>((fileOffset >> 32) & 0xFFFFFFFF)},
                                       hFile,
                                       std::move(createStateResult.value().promise),
                                       std::move(ref)
        );
        if (!token) {
            return std::unexpected{RuntimeResultCode::allocation_failure};
        }

        // 3. Уведомляем пул потоков Windows
        StartThreadpoolIo(ptpIo);

        BOOL result = ReadFile(
            token->m_hFile,
            dstBuffer,
            dstBufferSize, // Приводим к DWORD для Win32
            nullptr,
            &token->m_overlapped
        );

        if (!result) {
            DWORD error = GetLastError();
            if (error == ERROR_IO_PENDING) {
                // Штатный асинхронный запуск. Колбэк сработает позже.
                return AsyncIo{std::move(createStateResult.value().future), token};
            }

            // Произошла аппаратная или системная ошибка запуска операции
            CancelThreadpoolIo(ptpIo); // Обязательный баланс для пула

            auto statusCode = mapWin32Error(error);
            token->m_ioPromise.setValue({std::unexpected{statusCode}});
            delete token; // Удаляем токен, так как колбэка никогда не будет

            return std::unexpected{statusCode};
        }

        // Если result == TRUE (мгновенный успех):
        // Win32 Thread Pool все равно вызовет колбэк, поэтому ведем себя точно так же,
        // как при ERROR_IO_PENDING.
        return AsyncIo{std::move(createStateResult.value().future), token};
    }

    std::expected<AsyncIo, RuntimeResultCode> AsyncIoToken::startWrite(HANDLE hFile, PTP_IO ptpIo,
                                                                       const void *srcBuffer,
                                                                       size_t srcBufferSize, size_t fileOffset,
                                                                       IntrusiveStrongRef<IAsyncFileToken> &&ref) {
        // 1. Создаем SharedState для передачи статуса завершения (или количества записанных байт)
        auto createStateResult = AsyncStateFactory<IoStatus>::create();
        if (!createStateResult.has_value()) {
            return std::unexpected{RuntimeResultCode::allocation_failure};
        }

        // 2. Выделяем Токен (память выделится через ваш кастомный пул благодаря operator new)
        auto* token = new AsyncIoToken({
                                           .Offset = static_cast<DWORD>(fileOffset & 0xFFFFFFFF),
                                           .OffsetHigh = static_cast<DWORD>((fileOffset >> 32) & 0xFFFFFFFF)},
                                       hFile,
                                       std::move(createStateResult.value().promise),
                                       std::move(ref)
        );
        if (!token) {
            return std::unexpected{RuntimeResultCode::allocation_failure};
        }

        // 3. Уведомляем Windows Thread Pool ДО вызова системной функции
        StartThreadpoolIo(ptpIo);

        BOOL result = WriteFile(
            token->m_hFile,
            srcBuffer,
            srcBufferSize, // Win32 API строго требует DWORD
            nullptr,       // Для асинхронной записи здесь всегда nullptr
            &token->m_overlapped
        );

        if (!result) {
            DWORD error = GetLastError();
            if (error == ERROR_IO_PENDING) {
                // Операция успешно ушла в очередь ОС, колбэк выполнится позже
                return AsyncIo{std::move(createStateResult.value().future), token};
            }

            // Произошла критическая ошибка старта (например, диск переполнен или плохой HANDLE)
            // ВАЖНО: Балансируем счетчик пула, так как колбэка не будет
            CancelThreadpoolIo(ptpIo);

            auto statusCode = mapWin32Error(error);
            token->m_ioPromise.setValue({std::unexpected{statusCode}});
            delete token; // Сами очищаем токен

            return std::unexpected{statusCode};
        }

        // Если result == TRUE (данные мгновенно ушли в кэш ОС):
        // По спецификации Windows Thread Pool, колбэк все равно сработает,
        // поэтому возвращаем Future точно так же, как при асинхронном ожидании.
        return AsyncIo{std::move(createStateResult.value().future), token};
    }

    void AsyncIoToken::cancel() {
        CancelIoEx(m_hFile, &m_overlapped);
    }

    void AsyncIoToken::Win32IoCompletionCallback(PTP_CALLBACK_INSTANCE instance, PVOID,
                                                 PVOID overlapped, ULONG ioResult, ULONG_PTR bytes, PTP_IO) {
        auto* asyncIoToken = CONST_CONTAINING_RECORD(overlapped, AsyncIoToken, m_overlapped);
        auto statusCode = mapWin32Error(ioResult);
        if (statusCode != RuntimeResultCode::success) {
            asyncIoToken->m_ioPromise.setValue({std::unexpected{statusCode}});
        }
        else {
            asyncIoToken->m_ioPromise.setValue({bytes});
        }

        DisassociateCurrentThreadFromCallback(instance);

        delete asyncIoToken;
    }
}
