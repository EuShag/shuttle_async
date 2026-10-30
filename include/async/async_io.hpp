//
// Created by Shagu on 23.10.2026.
//

#ifndef SHUTTLE_ASYNC_ASYNC_IO_HPP
#define SHUTTLE_ASYNC_ASYNC_IO_HPP
#include "async_shared_state.hpp"
#include "intrusive_ptr.hpp"
#include "i_cancellable.hpp"
#include "runtime_result_code.hpp"

namespace shuttle::async {

    using IoStatus = std::expected<size_t, RuntimeResultCode>;

    class IAsyncIoToken;
    class IAsyncFileToken;

    class AsyncIo final : public ICancellable {
    public:
        AsyncIo(AsyncFuture<IoStatus>&& future, IAsyncIoToken* token) : m_token{token}, m_ioFuture{std::move(future)} {}

        void cancel() override;

        AsyncFuture<IoStatus>::FutureAwaiter get(IExecutor* executor);
        AsyncFuture<IoStatus>::FutureAwaiter get(){
            return get(IExecutor::getThreadPinnedExecutor());
        }

    private:
        IAsyncIoToken* m_token{};
        AsyncFuture<IoStatus> m_ioFuture{};
    };

    class IAsyncFileToken {
    public:
        virtual std::expected<AsyncIo, RuntimeResultCode> read(void *buff, size_t size, size_t offset, IntrusiveStrongRef<IAsyncFileToken>&& ref) = 0;
        virtual std::expected<AsyncIo, RuntimeResultCode> write(void const* buff, size_t size, size_t offset, IntrusiveStrongRef<IAsyncFileToken>&& ref) = 0;
        virtual ~IAsyncFileToken() = default;
    };

    // 1. Класс AsyncFile (Стековая RAII обертка) должен идти ПЕРВЫМ
    class AsyncFile {
    public:
        // Конструктор забирает перемещенный guard, владеющий токеном
        explicit AsyncFile(IntrusivePtr<IAsyncFileToken>&& owner) noexcept;

        AsyncFile(AsyncFile&&) noexcept = default;

        AsyncFile(const AsyncFile&) = delete;
        AsyncFile& operator=(const AsyncFile&) = delete;

        ~AsyncFile() = default; // Автоматически вызовет деструктор m_guard

        std::expected<AsyncIo, RuntimeResultCode> read(void *buff, size_t size, size_t offset) {
            auto ref = m_intrusiveFileTokenPtr.tryGetStrongRef();
            if (!ref.has_value()) return std::unexpected{RuntimeResultCode::cannot_open_file};

            return m_intrusiveFileTokenPtr->read(buff, size, offset, std::move(*ref));
        }

        std::expected<AsyncIo, RuntimeResultCode> write(void const *buff, size_t size, size_t offset) {
            auto ref = m_intrusiveFileTokenPtr.tryGetStrongRef();
            if (!ref.has_value()) return std::unexpected{RuntimeResultCode::cannot_open_file};

            return m_intrusiveFileTokenPtr->write(buff, size, offset, std::move(*ref));
        }


    private:
        IntrusivePtr<IAsyncFileToken> m_intrusiveFileTokenPtr;
    };

    class IAsyncIoToken {
    public:
        virtual ~IAsyncIoToken() = default;
        virtual void cancel() = 0;
    };
}

#endif //SHUTTLE_ASYNC_ASYNC_IO_HPP
