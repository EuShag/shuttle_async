//
// Created by Shagu on 30.10.2026.
//
#include "async/platform/threadpool_win32/utils.hpp"

namespace shuttle::async::threadpool {
    constexpr RuntimeResultCode mapWin32Error(DWORD win32Error) noexcept {
        switch (win32Error) {
            case ERROR_SUCCESS:                  return RuntimeResultCode::success;

            // --- Общие ---
            case ERROR_INVALID_PARAMETER:        return RuntimeResultCode::invalid_argument;
            case ERROR_ACCESS_DENIED:            return RuntimeResultCode::access_denied;
            case ERROR_NOT_ENOUGH_MEMORY:        return RuntimeResultCode::not_enough_memory;
            case ERROR_NOT_SUPPORTED:            return RuntimeResultCode::not_supported;
            case ERROR_INVALID_HANDLE:           return RuntimeResultCode::invalid_handle;

            // --- Файлы ---
            case ERROR_FILE_NOT_FOUND:           return RuntimeResultCode::file_not_found;
            case ERROR_PATH_NOT_FOUND:           return RuntimeResultCode::path_not_found;
            case ERROR_ALREADY_EXISTS:           return RuntimeResultCode::file_already_exists;
            case ERROR_FILE_EXISTS:              return RuntimeResultCode::file_already_exists;
            case ERROR_SHARING_VIOLATION:        return RuntimeResultCode::sharing_violation;
            case ERROR_LOCK_VIOLATION:           return RuntimeResultCode::lock_violation;
            case ERROR_IO_DEVICE:                return RuntimeResultCode::io_device_error;
            case ERROR_HANDLE_EOF:               return RuntimeResultCode::handle_eof;
            case ERROR_IO_INCOMPLETE:            return RuntimeResultCode::io_incomplete;
            case ERROR_IO_PENDING:               return RuntimeResultCode::io_pending;
            case ERROR_OPEN_FAILED:              return RuntimeResultCode::cannot_open_file;

            // --- Threadpool API ---
            case ERROR_NO_SYSTEM_RESOURCES:      return RuntimeResultCode::threadpool_quota_exceeded;
            case ERROR_FLT_NOT_INITIALIZED:      return RuntimeResultCode::threadpool_submission_failed;
            // Примечание: Для отмены или сбоев пула Windows часто возвращает ERROR_CANCELLED
            case ERROR_CANCELLED:                return RuntimeResultCode::callback_disabled;

            // --- События и Синхронизация ---
            case WAIT_TIMEOUT:                   return RuntimeResultCode::wait_timeout;
            case ERROR_TOO_MANY_POSTS:           return RuntimeResultCode::semaphore_limit_exceeded;
            case ERROR_ALREADY_ASSIGNED:         return RuntimeResultCode::event_already_signaled;
            case ERROR_SEM_TIMEOUT:              return RuntimeResultCode::wait_timeout;

            default:
                return RuntimeResultCode::unknown_error;
        }
    }
}
