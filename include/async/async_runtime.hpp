/**
 * @file async_runtime.hpp
 * @brief Platform-agnostic file management types and bitmasks.
 */

#ifndef SHUTTLE_ASYNC_ASYNC_RUNTIME_HPP
#define SHUTTLE_ASYNC_ASYNC_RUNTIME_HPP
#include <expected>
#include <filesystem>
#include "async_io.hpp"

namespace shuttle::async {

    /**
     * @enum FileAccessMode
     * @brief Specifies the data access rights requested for the file.
     */
    enum class FileAccessMode {
        read_only,  ///< Request read-only access. Maps to GENERIC_READ.
        write_only, ///< Request write-only access. Maps to GENERIC_WRITE.
        read_write  ///< Request both read and write access. Maps to GENERIC_READ | GENERIC_WRITE.
    };

    /**
     * @enum FileDisposition
     * @brief Specifies the action to take on a file that exists or does not exist.
     */
    enum class FileDisposition {
        open_existing,    ///< Opens a file only if it already exists. Fails if the file does not exist.
        open_always,      ///< Opens a file if it exists. If it does not exist, a new file is created.
        create_new,       ///< Creates a new file. Fails if the file already exists.
        create_always,    ///< Creates a new file. If the file exists, it overwrites and truncates it.
        truncate_existing ///< Opens an existing file and truncates its size to zero bytes. Requires write access.
    };

    /**
     * @enum FileShareMode
     * @brief Defines the sharing mode of the file, controlling concurrent access by other processes.
     * @note This enum can be used as a bitmask.
     */
    enum class FileShareMode {
        none         = 0,       ///< Prevents other processes from opening the file if they request read, write, or delete access.
        share_read   = 1 << 0,  ///< Enables subsequent open operations on the file to request read access.
        share_write  = 1 << 1,  ///< Enables subsequent open operations on the file to request write access.
        share_delete = 1 << 2   ///< Enables subsequent open operations on the file to request delete access or renaming.
    };

    /**
     * @enum FileFlags
     * @brief Represents file system attributes and performance optimization flags.
     * @note This enum acts as a bitmask. Combine values using bitwise OR (|).
     */
    enum class FileFlags : unsigned int {
        none            = 0,       ///< Default behavior without any specific flags or attributes.

        // --- OS Attributes ---
        attribute_normal= 1 << 0,  ///< The file does not have other attributes set. Valid only when used alone.
        hidden          = 1 << 1,  ///< The file is hidden. It is not included in an ordinary directory listing.
        temporary       = 1 << 2,  ///< The file is being used for temporary storage. File systems avoid flushing data back to mass storage if possible.
        delete_on_close = 1 << 3,  ///< The file is deleted immediately after all of its handles are closed.

        // --- I/O Optimization Flags ---
        asynchronous    = 1 << 4,  ///< Enables asynchronous/overlapped I/O operations. Required for integration with Win32 ThreadPool (PTP_IO).
        sequential_scan = 1 << 5,  ///< Hints that access is intended to be sequential from beginning to end. Optimizes system read-ahead caching.
        random_access   = 1 << 6,  ///< Hints that access is intended to be random. Disables aggressive system read-ahead caching.
        write_through   = 1 << 7,  ///< Forces the system to write through any intermediate cache and go directly to the physical storage.
        no_buffering    = 1 << 8   ///< Disables system-level caching. Requires sector-aligned buffers and transfer sizes.
    };

    // --- Bitmask Operator Overloads for C++20 Clean Usage ---

    /**
     * @brief Bitwise OR operator for combining FileShareMode flags.
     */
    constexpr FileShareMode operator|(FileShareMode a, FileShareMode b) {
        return static_cast<FileShareMode>(static_cast<int>(a) | static_cast<int>(b));
    }

    /**
     * @brief Bitwise AND operator for checking FileShareMode flags.
     */
    constexpr FileShareMode operator&(FileShareMode a, FileShareMode b) {
        return static_cast<FileShareMode>(static_cast<int>(a) & static_cast<int>(b));
    }

    /**
     * @brief Bitwise OR operator for combining FileFlags.
     */
    constexpr FileFlags operator|(FileFlags a, FileFlags b) {
        return static_cast<FileFlags>(static_cast<unsigned int>(a) | static_cast<unsigned int>(b));
    }

    /**
     * @brief Bitwise AND operator for checking FileFlags.
     */
    constexpr FileFlags operator&(FileFlags a, FileFlags b) {
        return static_cast<FileFlags>(static_cast<unsigned int>(a) & static_cast<unsigned int>(b));
    }

    /**
     * @struct FileOptions
     * @brief Aggregates configurations, access rights, and flags required to open or create a file.
     *
     * This structure leverages C++20 designated initializers for clean, readable, and
     * highly customizable file creation/opening calls.
     */
    struct FileOptions {
        /**
         * @brief Requested data access rights for the file.
         * @details Defaults to FileAccessMode::read_only.
         */
        FileAccessMode access = FileAccessMode::read_only;

        /**
         * @brief Action to take depending on whether the file already exists.
         * @details Defaults to FileDisposition::open_existing.
         */
        FileDisposition disposition = FileDisposition::open_existing;

        /**
         * @brief Concurrency sharing rules that control access for other processes.
         * @details Defaults to FileShareMode::share_read, allowing others to read concurrently.
         */
        FileShareMode share = FileShareMode::share_read;

        /**
         * @brief Low-level system attributes and hardware/kernel I/O optimization flags.
         * @details Defaults to FileFlags::attribute_normal. Can be combined using bitwise OR (|).
         */
        FileFlags flags = FileFlags::attribute_normal;
    };


    class IAsyncRuntime {
    public:
        virtual std::expected<AsyncFile, FileAccessMode> createFile(
            std::filesystem::path path,
            FileOptions options
        ) = 0;

        virtual std::expected<AsyncIo, FileAccessMode> createTimer() = 0;
        virtual std::expected<AsyncIo, FileAccessMode> createEvent() = 0;

        virtual ~IAsyncRuntime() = default;
    };


}

#endif //SHUTTLE_ASYNC_ASYNC_RUNTIME_HPP
