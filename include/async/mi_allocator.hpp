#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>

namespace shuttle::async {

    /**
     * @brief Counts allocations registered by MiAllocator on the current thread.
     */
    struct MiAllocationStats {
        /// Number of successful allocation requests during the observation period.
        uint64_t allocations{0};
        /// Total number of bytes requested.
        uint64_t bytes{0};
    };

    /**
     * @brief Base allocation operations using mimalloc.
     *
     * The allocation counter is thread-local; it observes only calls made on
     * the thread where an AllocationCounterScope is active.
     */
    class MiAllocatorBase {
    public:
        /// Allocates an object using mimalloc; returns `nullptr` on failure.
        static void* operator new(std::size_t size) noexcept;
        /// Frees memory allocated by the corresponding operator new.
        static void operator delete(void* ptr) noexcept;

        /**
         * @brief RAII registration of an allocation counter for the current thread.
         *
         * Nested scopes correctly restore the previously active counter.
         */
        class AllocationCounterScope {
        public:
            /// Starts recording statistics in stats until this scope is destroyed.
            /// @param stats Statistics object that must outlive this scope.
            explicit AllocationCounterScope(MiAllocationStats& stats) noexcept
                : m_previous{exchangeAllocationCounter(&stats)} {}

            AllocationCounterScope(const AllocationCounterScope&) = delete;
            AllocationCounterScope& operator=(const AllocationCounterScope&) = delete;

            /// Restores the previously active counter.
            ~AllocationCounterScope() {
                static_cast<void>(exchangeAllocationCounter(m_previous));
            }

        private:
            /// Previous TLS counter, restored when nested scopes are exited.
            MiAllocationStats* m_previous;
        };

    protected:
        /// Constructs the stateless allocator base.
        MiAllocatorBase() noexcept = default;

        /// Non-virtual: the base type does not own derived allocators.
        ~MiAllocatorBase() noexcept = default;

        /// Allocates raw memory with the requested size and alignment.
        /// @return Pointer to the memory, or `nullptr` on failure.
        [[nodiscard]] void* allocate_bytes(std::size_t bytes, std::size_t alignment) noexcept;

        /// Frees memory allocated by allocate_bytes.
        void deallocate_bytes(void* ptr) noexcept;

    private:
        /// Replaces the active thread-local counter and returns the previous one.
        static MiAllocationStats* exchangeAllocationCounter(
            MiAllocationStats* stats) noexcept;
        /// Non-owning pointer to the current thread's statistics.
        inline static thread_local MiAllocationStats* m_allocationStats{nullptr};
    };

    /**
     * @brief Stateless allocator for objects of type T, based on mimalloc.
     *
     * Allocation is noexcept and returns `nullptr` on failure (unlike a
     * standard allocator, which typically throws bad_alloc). Instances are
     * interchangeable when deallocating memory.
     * @tparam T Type of the allocated element.
     */
    template <typename T>
    class MiAllocator : MiAllocatorBase {
    public:
        /// Element type of the allocator.
        using value_type = T;

        /// Constructs an empty stateless allocator.
        constexpr MiAllocator() noexcept = default;

        /// Constructs an allocator for a compatible type U.
        /// @tparam U Type of the source allocator.
        template <typename U>
        constexpr MiAllocator(const MiAllocator<U>&) noexcept {}

        /**
         * @brief Allocates memory for n elements.
         * @param n Number of objects.
         * @return `nullptr` if n == 0 or allocation fails; otherwise, aligned memory.
         * @note Overflow of n * sizeof(T) is not explicitly checked here.
         */
        [[nodiscard]] T* allocate(std::size_t n) noexcept {
            if (n == 0) [[unlikely]] return nullptr;

            // Вычисляем размер и запрашиваем выровненные байты у базового класса
            void* p = this->allocate_bytes(n * sizeof(T), alignof(T));
            return static_cast<T*>(p);
        }

        /**
         * @brief Deallocates a block previously returned by allocate.
         * @param p Pointer to the block; `nullptr` is allowed.
         * @param size Number of elements (unused).
         */
        void deallocate(T* p, std::size_t size = 0) noexcept {
            if (p == nullptr) [[unlikely]] return;
            this->deallocate_bytes(p);
        }

    };

    /**
     * @brief Compares two MiAllocator instances of different types.
     * @return Always true because the allocators are stateless and interchangeable.
     */
    template <typename T, typename U>
    [[nodiscard]] constexpr bool operator==(const MiAllocator<T>&, const MiAllocator<U>&) noexcept {
        return true;
    }

    /**
     * @brief Negates the comparison of two MiAllocator instances.
     * @return Always false because all MiAllocator instances compare equal.
     */
    template <typename T, typename U>
    [[nodiscard]] constexpr bool operator!=(const MiAllocator<T>&, const MiAllocator<U>&) noexcept {
        return false;
    }

    /**
     * @brief Calls the object's destructor and frees its memory using MiAllocator.
     * @tparam T Object type.
     * @param ptr Pointer to a live object; `nullptr` is ignored.
     */
    template <typename T>
    struct MiDeleter {
        /// Destroys the object and deallocates its memory block.
        void operator()(T* ptr) const noexcept {
            if (ptr == nullptr) [[unlikely]] return;

            // 1. Явно вызываем деструктор объекта типа T
            ptr->~T();

            // 2. Освобождаем память через наш MiAllocator
            MiAllocator<T> alloc;
            alloc.deallocate(ptr, 1);
        }
    };

    /**
     * @brief Unique owning pointer with MiDeleter.
     * @tparam T Object type.
     */
    template <typename T>
    using MiUniquePtr = std::unique_ptr<T, MiDeleter<T>>;

    /**
     * @brief Constructs an object of type T without throwing, using MiAllocator.
     * @tparam T Type of the object to construct.
     * @tparam Args Types of the constructor arguments.
     * @param args Constructor arguments, passed using perfect forwarding.
     * @return Unique pointer, or an empty pointer if allocation fails.
     * @note Compilation requires T to have a noexcept constructor for the given Args.
     */
    template <typename T, typename... Args>
    [[nodiscard]] MiUniquePtr<T> mi_make_unique_nothrow(Args&&... args) noexcept {
        // ЖЕСТКИЙ СТАТИЧЕСКИЙ ПРЕДОХРАНИТЕЛЬ:
        // Запрещаем сборку, если конструктор типа T с заданными аргументами может бросить исключение!
        static_assert(std::is_nothrow_constructible_v<T, Args...>,
            "Critical Error: Type T must be completely noexcept constructible with provided arguments!");

        MiAllocator<T> alloc;

        // 1. Выделяем сырую память под один объект типа T через мималлок
        T* memory = alloc.allocate(1);

        // Предохранитель на случай тотальной нехватки RAM на хосте
        if (!memory) [[unlikely]] {
            return MiUniquePtr<T>(nullptr);
        }

        // 2. Конструируем объект прямо по вычисленному адресу (Placement New).
        // Благодаря static_assert выше, мы на 100% уверены, что эта строчка безопасна.
        T* ptr = ::new (static_cast<void*>(memory)) T(std::forward<Args>(args)...);

        // 3. Упаковываем в наш кастомный умный указатель и возвращаем по семантике перемещения
        return MiUniquePtr<T>(ptr);
    }
} // namespace shuttle::async
