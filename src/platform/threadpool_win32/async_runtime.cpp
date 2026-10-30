//
// Created by Shagu on 30.10.2026.
//
#include "async/platform/threadpool_win32/async_runtime.hpp"

namespace shuttle::async::threadpool {

    /**
     * @brief Транслирует кроссплатформенные FileOptions в нативные аргументы Win32 CreateFile.
     */
    constexpr Win32FileArguments translateFileOptionsToWin32(const FileOptions& options) {
        Win32FileArguments result{};

        // 1. Маппинг прав доступа (Desired Access)
        switch (options.access) {
            case FileAccessMode::read_only:  result.desired_access = GENERIC_READ;  break;
            case FileAccessMode::write_only: result.desired_access = GENERIC_WRITE; break;
            case FileAccessMode::read_write: result.desired_access = GENERIC_READ | GENERIC_WRITE; break;
        }

        // 2. Маппинг правил разделяемого доступа (Share Mode)
        result.share_mode = 0;
        if ((options.share & FileShareMode::share_read) != FileShareMode::none)   result.share_mode |= FILE_SHARE_READ;
        if ((options.share & FileShareMode::share_write) != FileShareMode::none)  result.share_mode |= FILE_SHARE_WRITE;
        if ((options.share & FileShareMode::share_delete) != FileShareMode::none) result.share_mode |= FILE_SHARE_DELETE;

        // 3. Маппинг стратегии открытия файла (Creation Disposition)
        switch (options.disposition) {
            case FileDisposition::open_existing:    result.creation_disposition = OPEN_EXISTING;    break;
            case FileDisposition::open_always:      result.creation_disposition = OPEN_ALWAYS;      break;
            case FileDisposition::create_new:       result.creation_disposition = CREATE_NEW;       break;
            case FileDisposition::create_always:    result.creation_disposition = CREATE_ALWAYS;    break;
            case FileDisposition::truncate_existing: result.creation_disposition = TRUNCATE_EXISTING; break;
        }

        // 4. Маппинг атрибутов и флагов оптимизации ввода-вывода (Flags and Attributes)
        result.flags_and_attributes = 0;

        // Атрибуты ОС
        if ((options.flags & FileFlags::attribute_normal) != FileFlags::none) result.flags_and_attributes |= FILE_ATTRIBUTE_NORMAL;
        if ((options.flags & FileFlags::hidden) != FileFlags::none)           result.flags_and_attributes |= FILE_ATTRIBUTE_HIDDEN;
        if ((options.flags & FileFlags::temporary) != FileFlags::none)        result.flags_and_attributes |= FILE_ATTRIBUTE_TEMPORARY;
        if ((options.flags & FileFlags::delete_on_close) != FileFlags::none)  result.flags_and_attributes |= FILE_FLAG_DELETE_ON_CLOSE;

        // Флаги оптимизации ввода-вывода
        if ((options.flags & FileFlags::asynchronous) != FileFlags::none)    result.flags_and_attributes |= FILE_FLAG_OVERLAPPED;
        if ((options.flags & FileFlags::sequential_scan) != FileFlags::none) result.flags_and_attributes |= FILE_FLAG_SEQUENTIAL_SCAN;
        if ((options.flags & FileFlags::random_access) != FileFlags::none)   result.flags_and_attributes |= FILE_FLAG_RANDOM_ACCESS;
        if ((options.flags & FileFlags::write_through) != FileFlags::none)   result.flags_and_attributes |= FILE_FLAG_WRITE_THROUGH;
        if ((options.flags & FileFlags::no_buffering) != FileFlags::none)    result.flags_and_attributes |= FILE_FLAG_NO_BUFFERING;

        return result;
    }

    std::expected<AsyncFile, RuntimeResultCode> AsyncRuntime::createFile(std::filesystem::path const &path,
        FileOptions options, IntrusiveStrongRef<IAsyncRuntime>&& ref) {

        if (!tryAcquire()) return std::unexpected{RuntimeResultCode::cannot_open_file};
        // Если мы дошли сюда, ссылка ГАРАНТИРОВАННО и безопасно захвачена.
        // Никаких откатов (fetch_sub) делать не нужно.

        auto win32FileOptions = translateFileOptionsToWin32(options);

        // ВАЖНО: Не забудьте обернуть создание AsyncFile так, чтобы в случае исключения
        // или ошибки внутри этого блока, ссылка безопасно освобождалась (вызывался аналог release()).
        return AsyncFileToken::create(
            path,
            win32FileOptions.desired_access,
            win32FileOptions.share_mode,
            win32FileOptions.creation_disposition,
            win32FileOptions.flags_and_attributes,
            m_callbackEnviron.get()
        );
    }
}
