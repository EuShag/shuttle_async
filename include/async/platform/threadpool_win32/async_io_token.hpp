//
// Created by Shagu on 22.10.2026.
//

#ifndef SHUTTLE_ASYNC_ASYNCIOTOKEN_HPP
#define SHUTTLE_ASYNC_ASYNCIOTOKEN_HPP

#include <exception>
#include <Windows.h>
#include "async/async_io.hpp"

namespace shuttle::async::threadpool {

    /**
         * @brief Translates a low-level Win32 error code into a high-level IoResultCode.
         *
         * @param win32_error The standard Windows error code (typically obtained via GetLastError()
         *                    or from the thread pool callback).
         * @return IoResultCode The corresponding domain-specific status code.
         *
         * @note ERROR_IO_PENDING must be handled manually prior to calling this mapper,
         *       as it represents an active asynchronous state rather than a terminal failure.
         */
        static IoResultCode mapWin32Error(ULONG win32_error) {
            switch (win32_error) {
                case NO_ERROR:                      return IoResultCode::success;
                case ERROR_HANDLE_EOF:              return IoResultCode::end_of_file;
                case ERROR_MORE_DATA:               return IoResultCode::more_data_available;

                // Paths and permissions
                case ERROR_FILE_NOT_FOUND:
                case ERROR_PATH_NOT_FOUND:          return IoResultCode::cannot_open_file;
                case ERROR_ACCESS_DENIED:           return IoResultCode::access_denied;
                case ERROR_INVALID_HANDLE:          return IoResultCode::invalid_file_token;
                case ERROR_SHARING_VIOLATION:       return IoResultCode::sharing_violation;
                case ERROR_LOCK_VIOLATION:          return IoResultCode::lock_violation;

                // Lifecycle and cancellation
                case ERROR_OPERATION_ABORTED:       return IoResultCode::operation_aborted;

                // Storage and hardware issues
                case ERROR_DISK_FULL:
                case ERROR_HANDLE_DISK_FULL:        return IoResultCode::disk_full;
                case ERROR_SECTOR_NOT_FOUND:
                case ERROR_CRC:                     return IoResultCode::hardware_error;
                case ERROR_WRITE_FAULT:             return IoResultCode::write_fault;
                case ERROR_TOO_MANY_OPEN_FILES:     return IoResultCode::too_many_open_files;

                // Network and timeouts
                case ERROR_DEV_NOT_EXIST:           return IoResultCode::network_error;
                case ERROR_NETWORK_BUSY:            return IoResultCode::network_busy;
                case ERROR_SEM_TIMEOUT:             return IoResultCode::timeout;

                // Named Pipes
                case ERROR_BROKEN_PIPE:             return IoResultCode::broken_pipe;
                case ERROR_PIPE_BUSY:               return IoResultCode::pipe_busy;
                case ERROR_PIPE_NOT_CONNECTED:      return IoResultCode::pipe_not_connected;

                // Kernel memory resources and parameter bugs
                case ERROR_NOT_ENOUGH_MEMORY:
                case ERROR_NO_SYSTEM_RESOURCES:     return IoResultCode::system_resources_exhausted;
                case ERROR_INVALID_PARAMETER:       return IoResultCode::invalid_parameter;
                case ERROR_INSUFFICIENT_BUFFER:     return IoResultCode::insufficient_buffer;

                default:                            return IoResultCode::unknown;
            }
        }

    class AsyncIoToken final : public IAsyncIoToken {
    public:
        [[nodiscard]] static std::expected<AsyncIo, IoResultCode> startRead(
            HANDLE hFile, PTP_IO ptpIo, void* dstBuffer, size_t dstBufferSize, size_t fileOffset, AsyncFileTokenGuard guard
        ) {
            // 1. Выделяем SharedState для передачи данных
            auto createStateResult = AsyncStateFactory<IoStatus>::create();
            if (!createStateResult.has_value()) {
                return std::unexpected{IoResultCode::allocation_failed};
            }

            // 2. Выделяем Токен (теперь из нашего быстрого PMR пул-аллокатора благодаря переопределению new)
            auto* token = new AsyncIoToken({
                .Offset = static_cast<DWORD>(fileOffset & 0xFFFFFFFF),
                .OffsetHigh = static_cast<DWORD>((fileOffset >> 32) & 0xFFFFFFFF)},
                hFile,
                std::move(createStateResult.value().promise),
                std::move(guard)
            );
            if (!token) {
                return std::unexpected{IoResultCode::allocation_failed};
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


        [[nodiscard]] static std::expected<AsyncIo, IoResultCode> startWrite(
            HANDLE hFile, PTP_IO ptpIo, const void* srcBuffer, size_t srcBufferSize, size_t fileOffset, AsyncFileTokenGuard guard
        ) {
            // 1. Создаем SharedState для передачи статуса завершения (или количества записанных байт)
            auto createStateResult = AsyncStateFactory<IoStatus>::create();
            if (!createStateResult.has_value()) {
                return std::unexpected{IoResultCode::allocation_failed};
            }

            // 2. Выделяем Токен (память выделится через ваш кастомный пул благодаря operator new)
            auto* token = new AsyncIoToken({
                .Offset = static_cast<DWORD>(fileOffset & 0xFFFFFFFF),
                .OffsetHigh = static_cast<DWORD>((fileOffset >> 32) & 0xFFFFFFFF)},
                hFile,
                std::move(createStateResult.value().promise),
                std::move(guard)
            );
            if (!token) {
                return std::unexpected{IoResultCode::allocation_failed};
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

        AsyncIoToken(OVERLAPPED const &overlapped, HANDLE hFile, AsyncPromise<IoStatus>&& promise, AsyncFileTokenGuard guard)
            : m_overlapped{overlapped}, m_hFile{hFile}, m_ioPromise{std::move(promise)}, m_asyncFileTokenGuard{std::move(guard)} {}

        void cancel() override {
            CancelIoEx(m_hFile, &m_overlapped);
        }

        static void CALLBACK Win32IoCompletionCallback(
            PTP_CALLBACK_INSTANCE,
            PVOID,
            PVOID overlapped,
            ULONG ioResult,
            ULONG_PTR bytes,
            PTP_IO
        ) {
            auto* asyncIoToken = CONTAINING_RECORD(overlapped, AsyncIoToken, m_overlapped);
            auto statusCode = mapWin32Error(ioResult);
            if (statusCode != IoResultCode::success) {
                asyncIoToken->m_ioPromise.setValue({std::unexpected{statusCode}});
            }
            else {
                asyncIoToken->m_ioPromise.setValue({bytes});
            }

            delete asyncIoToken;
        }

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

    private:
        OVERLAPPED m_overlapped{};
        HANDLE m_hFile{};
        AsyncPromise<IoStatus> m_ioPromise;
        AsyncFileTokenGuard m_asyncFileTokenGuard;
    };


}

#endif //SHUTTLE_ASYNC_ASYNCIOTOKEN_HPP