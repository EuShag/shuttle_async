//
// Created by Shagu on 23.10.2026.
//

#ifndef SHUTTLE_ASYNC_ASYNC_IO_HPP
#define SHUTTLE_ASYNC_ASYNC_IO_HPP
#include "async_shared_state.hpp"

namespace shuttle::async {

    /**
     * @brief Represents the high-level status or error codes for asynchronous I/O operations.
     *
     * This enumeration abstracts low-level Win32 system errors into a clean,
     * universal set of domain-specific result codes suitable for modern C++ coroutines.
     */
    enum class IoResultCode {
        // === SUCCESS AND PLANNING STATUSES ===
        /// @brief The I/O operation completed successfully.
        success,
        /// @brief Memory allocation failed on the C++ side (e.g., token or shared state creation).
        allocation_failed,
        /// @brief The buffer was too small; more data is available in the pipe/channel.
        more_data_available,

        // === ACCESS AND PATH ERRORS ===
        /// @brief The specified file or directory path could not be found.
        cannot_open_file,
        /// @brief Access to the file or directory was denied due to insufficient permissions.
        access_denied,
        /// @brief An invalid handle was used, or the token was malformed.
        invalid_file_token,
        /// @brief The file is locked by another process (sharing violation).
        sharing_violation,
        /// @brief The requested byte range is locked by another process via LockFileEx.
        lock_violation,

        // === OPERATION LIFECYCLE ===
        /// @brief The end of the file (EOF) has been reached.
        end_of_file,
        /// @brief The I/O operation was explicitly cancelled (e.g., via CancelIoEx).
        operation_aborted,

        // === HARDWARE AND DISK LIMITS ===
        /// @brief The disk is full or the user's storage quota has been exceeded.
        disk_full,
        /// @brief A physical hardware defect occurred (e.g., bad sector or CRC mismatch).
        hardware_error,
        /// @brief A physical fault occurred during a write sequence on the device.
        write_fault,
        /// @brief The system-wide or process limit for open file descriptors was exceeded.
        too_many_open_files,

        // === NETWORK AND TIMEOUTS (NAS / SMB / UNC Paths) ===
        /// @brief The network resource or physical storage device (e.g., USB drive) was disconnected.
        network_error,
        /// @brief The network is congested or temporarily unreachable.
        network_busy,
        /// @brief The operation timed out waiting for a response from a remote storage node.
        timeout,

        // === NAMED PIPES AND SOCKETS ===
        /// @brief The pipe handle was closed from the other side (broken pipe).
        broken_pipe,
        /// @brief All instances of the requested named pipe are currently busy.
        pipe_busy,
        /// @brief The pipe is waiting for a client connection via ConnectNamedPipe.
        pipe_not_connected,

        // === SYSTEM OS ERRORS ===
        /// @brief The OS kernel ran out of memory or non-paged pool resources for I/O tracking.
        system_resources_exhausted,
        /// @brief An invalid parameter was passed (e.g., an unaligned buffer with FILE_FLAG_NO_BUFFERING).
        invalid_parameter,
        /// @brief The buffer size is insufficient to hold system-defined structures.
        insufficient_buffer,
        /// @brief An unexpected system error occurred that is not covered by this enum.
        unknown
    };

    using IoStatus = std::expected<size_t, IoResultCode>;

    class IAsyncIoToken;
    class IAsyncFileToken;

    class AsyncIo {
    public:
        AsyncIo(AsyncFuture<IoStatus>&& future, IAsyncIoToken* token) : m_ioFuture{std::move(future)}, m_token{token} {}

        void cancel();

        AsyncFuture<IoStatus>::FutureAwaitable get(IExecutor* executor);
        AsyncFuture<IoStatus>::FutureAwaitable operator co_await(){
            return get(IExecutor::getThreadPinnedExecutor());
        }

    private:
        IAsyncIoToken* m_token{};
        AsyncFuture<IoStatus> m_ioFuture{};
    };

    class IAsyncFileToken {
    public:
        virtual std::expected<AsyncIo, IoResultCode> read(void *buff, size_t size, size_t offset) = 0;
        virtual std::expected<AsyncIo, IoResultCode> write(void const *buff, size_t size, size_t offset) = 0;
        virtual void acquire() = 0;
        virtual void release() = 0;

        virtual ~IAsyncFileToken() = default;
    };

    class AsyncFileTokenGuard {
    public:
        AsyncFileTokenGuard(IAsyncFileToken* token) : m_token{token} {
            if (m_token) {
                m_token->acquire();
            }
        }

        ~AsyncFileTokenGuard() {
            // release() вызовется ТОЛЬКО если m_token не занулен через dismiss()
            if (m_token) {
                m_token->release();
            }
        }

        // Запрещаем копирование, чтобы случайно не размножить guard
        AsyncFileTokenGuard(const AsyncFileTokenGuard&) = delete;
        AsyncFileTokenGuard& operator=(const AsyncFileTokenGuard&) = delete;

        /**
         * @brief Move constructor transfers the reference ownership to the new guard.
         *
         * It extracts the token pointer from the other guard and dismisses it,
         * ensuring the reference count remains stable and no premature release occurs.
         */
        AsyncFileTokenGuard(AsyncFileTokenGuard&& other) noexcept
            : m_token{other.m_token}
        {
            other.dismiss();
        }

        // Метод "Увольнения". Вызывается, когда операция успешно ушла в ОС.
        void dismiss() noexcept {
            m_token = nullptr;
        }

        // Добавляем доступ к сырому токену
        [[nodiscard]] IAsyncFileToken* get() const noexcept { return m_token; }
        IAsyncFileToken* operator->() const noexcept { return m_token; }

    private:
        IAsyncFileToken* m_token{};
    };

    // 1. Класс AsyncFile (Стековая RAII обертка) должен идти ПЕРВЫМ
    class AsyncFile {
    public:
        // Конструктор забирает перемещенный guard, владеющий токеном
        explicit AsyncFile(AsyncFileTokenGuard&& guard) noexcept;

        AsyncFile(AsyncFile&&) noexcept = default;

        AsyncFile(const AsyncFile&) = delete;
        AsyncFile& operator=(const AsyncFile&) = delete;

        ~AsyncFile() = default; // Автоматически вызовет деструктор m_guard

        std::expected<AsyncIo, IoResultCode> read(void *buff, size_t size, size_t offset);
        std::expected<AsyncIo, IoResultCode> write(void const *buff, size_t size, size_t offset);

    private:
        AsyncFileTokenGuard m_guard;
    };

    class IAsyncIoToken {
    public:
        virtual ~IAsyncIoToken() = default;
        virtual void cancel() = 0;
    };
}

#endif //SHUTTLE_ASYNC_ASYNC_IO_HPP
