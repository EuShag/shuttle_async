//
// Created by Shagu on 30.10.2026.
//

#ifndef SHUTTLE_ASYNC_RUNTIME_RESULT_CODE_HPP
#define SHUTTLE_ASYNC_RUNTIME_RESULT_CODE_HPP

namespace shuttle::async {
    enum class RuntimeResultCode : uint32_t {
        success = 0,

        // --- Общие ошибки ---
        unknown_error,
        invalid_argument,
        access_denied,
        not_enough_memory,
        not_supported,
        allocation_failure,

        // --- Ошибки файловой системы (Files & I/O) ---
        file_not_found,
        path_not_found,
        cannot_open_file,
        file_already_exists,
        sharing_violation,
        lock_violation,
        io_device_error,
        handle_eof,
        io_incomplete,
        io_pending,

        // --- Win32 Threadpool API ---
        threadpool_quota_exceeded,
        threadpool_submission_failed,
        threadpool_closed,
        callback_disabled,

        // --- Синхронизация и События (Events, Mutexes, Semaphores) ---
        event_already_signaled,
        wait_timeout,
        wait_abandoned,
        mutex_already_owned,
        semaphore_limit_exceeded,
        invalid_handle,

        // --- Системные Таймеры ---
        timer_invalid_period,
        timer_already_expired
    };
}

#endif //SHUTTLE_ASYNC_RUNTIME_RESULT_CODE_HPP
