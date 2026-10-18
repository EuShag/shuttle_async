#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>

namespace shuttle::async {

    /**
     * @if RU
     * @brief Счётчики аллокаций, зарегистрированных MiAllocator в текущем потоке.
     * @endif
     * @if EN
     * @brief Counts allocations registered by MiAllocator on the current thread.
     * @endif
     */
    struct MiAllocationStats {
        /// @if RU
        /// Число успешных запросов аллокации в периоде наблюдения.
        /// @endif
        /// @if EN
        /// Number of successful allocation requests during the observation period.
        /// @endif
        uint64_t allocations{0};
        /// @if RU
        /// Суммарное число запрошенных байтов.
        /// @endif
        /// @if EN
        /// Total number of bytes requested.
        /// @endif
        uint64_t bytes{0};
    };

    /**
     * @if RU
     * @brief Базовые операции аллокации через mimalloc.
     *
     * Счётчик аллокаций является thread-local; он наблюдает только вызовы,
     * выполненные в потоке, где активен AllocationCounterScope.
     * @endif
     * @if EN
     * @brief Base allocation operations using mimalloc.
     *
     * The allocation counter is thread-local; it observes only calls made on
     * the thread where an AllocationCounterScope is active.
     * @endif
     */
    class MiAllocatorBase {
    public:
        /// @if RU
        /// Выделяет объект через mimalloc; неудача возвращается как nullptr.
        /// @endif
        /// @if EN
        /// Allocates an object using mimalloc; returns `nullptr` on failure.
        /// @endif
        static void* operator new(std::size_t size) noexcept;
        /// @if RU
        /// Освобождает память, выделенную соответствующим operator new.
        /// @endif
        /// @if EN
        /// Frees memory allocated by the corresponding operator new.
        /// @endif
        static void operator delete(void* ptr) noexcept;

        /**
         * @if RU
         * @brief RAII-подключение счётчика аллокаций для текущего потока.
         *
         * Области вложения корректно восстанавливают предыдущий счётчик.
         * @endif
         * @if EN
         * @brief RAII registration of an allocation counter for the current thread.
         *
         * Nested scopes correctly restore the previously active counter.
         * @endif
         */
        class AllocationCounterScope {
        public:
            /// @if RU
            /// Начинает записывать статистику в stats до разрушения области.
            /// @param stats Объект статистики, который должен пережить эту область.
            /// @endif
            /// @if EN
            /// Starts recording statistics in stats until this scope is destroyed.
            /// @param stats Statistics object that must outlive this scope.
            /// @endif
            explicit AllocationCounterScope(MiAllocationStats& stats) noexcept
                : m_previous{exchangeAllocationCounter(&stats)} {}

            AllocationCounterScope(const AllocationCounterScope&) = delete;
            AllocationCounterScope& operator=(const AllocationCounterScope&) = delete;

            /// @if RU
            /// Восстанавливает ранее активный счётчик.
            /// @endif
            /// @if EN
            /// Restores the previously active counter.
            /// @endif
            ~AllocationCounterScope() {
                static_cast<void>(exchangeAllocationCounter(m_previous));
            }

        private:
            /// @if RU
            /// Предыдущий счётчик TLS для восстановления вложенных областей.
            /// @endif
            /// @if EN
            /// Previous TLS counter, restored when nested scopes are exited.
            /// @endif
            MiAllocationStats* m_previous;
        };

    protected:
        /// @if RU
        /// Конструирует базовый интерфейс allocator-а без состояния.
        /// @endif
        /// @if EN
        /// Constructs the stateless allocator base.
        /// @endif
        MiAllocatorBase() noexcept = default;
        /// @if RU
        /// Виртуальности нет: базовый тип не владеет производными allocator-ами.
        /// @endif
        /// @if EN
        /// Non-virtual: the base type does not own derived allocators.
        /// @endif
        ~MiAllocatorBase() noexcept = default;

        /// @if RU
        /// Выделяет сырую память требуемого размера и выравнивания.
        /// @return Указатель на память либо nullptr при ошибке.
        /// @endif
        /// @if EN
        /// Allocates raw memory with the requested size and alignment.
        /// @return Pointer to the memory, or `nullptr` on failure.
        /// @endif
        [[nodiscard]] void* allocate_bytes(std::size_t bytes, std::size_t alignment) noexcept;

        /// @if RU
        /// Освобождает память, выделенную allocate_bytes.
        /// @endif
        /// @if EN
        /// Frees memory allocated by allocate_bytes.
        /// @endif
        void deallocate_bytes(void* ptr) noexcept;

    private:
        /// @if RU
        /// Меняет активный thread-local счётчик и возвращает прежний.
        /// @endif
        /// @if EN
        /// Replaces the active thread-local counter and returns the previous one.
        /// @endif
        static MiAllocationStats* exchangeAllocationCounter(
            MiAllocationStats* stats) noexcept;
        /// @if RU
        /// Не владеющий указатель на статистику текущего потока.
        /// @endif
        /// @if EN
        /// Non-owning pointer to the current thread's statistics.
        /// @endif
        inline static thread_local MiAllocationStats* m_allocationStats{nullptr};
    };

    /**
     * @if RU
     * @brief Stateless allocator для объектов типа T на базе mimalloc.
     *
     * Аллокация noexcept и возвращает nullptr при неудаче (в отличие от
     * стандартного allocator, обычно бросающего bad_alloc). Экземпляры
     * взаимозаменяемы при освобождении памяти.
     * @tparam T Тип выделяемого элемента.
     * @endif
     * @if EN
     * @brief Stateless allocator for objects of type T, based on mimalloc.
     *
     * Allocation is noexcept and returns `nullptr` on failure (unlike a
     * standard allocator, which typically throws bad_alloc). Instances are
     * interchangeable when deallocating memory.
     * @tparam T Type of the allocated element.
     * @endif
     */
    template <typename T>
    class MiAllocator : MiAllocatorBase {
    public:
        /// @if RU
        /// Тип элементов allocator-а.
        /// @endif
        /// @if EN
        /// Element type of the allocator.
        /// @endif
        using value_type = T;

        /// @if RU
        /// Создаёт пустой stateless allocator.
        /// @endif
        /// @if EN
        /// Constructs an empty stateless allocator.
        /// @endif
        constexpr MiAllocator() noexcept = default;

        /// @if RU
        /// Создаёт allocator для совместимого типа U.
        /// @tparam U Тип исходного allocator-а.
        /// @endif
        /// @if EN
        /// Constructs an allocator for a compatible type U.
        /// @tparam U Type of the source allocator.
        /// @endif
        template <typename U>
        constexpr MiAllocator(const MiAllocator<U>&) noexcept {}

        /**
         * @if RU
         * @brief Выделяет память для n элементов.
         * @param n Количество объектов.
         * @return nullptr при n == 0 или ошибке; иначе выровненная память.
         * @note Проверка переполнения n * sizeof(T) здесь явно не выполняется.
         * @endif
         * @if EN
         * @brief Allocates memory for n elements.
         * @param n Number of objects.
         * @return `nullptr` if n == 0 or allocation fails; otherwise, aligned memory.
         * @note Overflow of n * sizeof(T) is not explicitly checked here.
         * @endif
         */
        [[nodiscard]] T* allocate(std::size_t n) noexcept {
            if (n == 0) [[unlikely]] return nullptr;

            // Вычисляем размер и запрашиваем выровненные байты у базового класса
            void* p = this->allocate_bytes(n * sizeof(T), alignof(T));
            return static_cast<T*>(p);
        }

        /**
         * @if RU
         * @brief Освобождает блок, ранее выданный allocate.
         * @param p Указатель на блок; nullptr допустим.
         * @param size Количество элементов (не используется).
         * @endif
         * @if EN
         * @brief Deallocates a block previously returned by allocate.
         * @param p Pointer to the block; `nullptr` is allowed.
         * @param size Number of elements (unused).
         * @endif
         */
        void deallocate(T* p, std::size_t size = 0) noexcept {
            if (p == nullptr) [[unlikely]] return;
            this->deallocate_bytes(p);
        }

    };

    /**
     * @if RU
     * @brief Сравнивает два MiAllocator разных типов.
     * @return Всегда true, поскольку allocator stateless и взаимозаменяем.
     * @endif
     * @if EN
     * @brief Compares two MiAllocator instances of different types.
     * @return Always true because the allocators are stateless and interchangeable.
     * @endif
     */
    template <typename T, typename U>
    [[nodiscard]] constexpr bool operator==(const MiAllocator<T>&, const MiAllocator<U>&) noexcept {
        return true;
    }

    /**
     * @if RU
     * @brief Отрицание сравнения двух MiAllocator.
     * @return Всегда false, поскольку любые экземпляры MiAllocator равны.
     * @endif
     * @if EN
     * @brief Negates the comparison of two MiAllocator instances.
     * @return Always false because all MiAllocator instances compare equal.
     * @endif
     */
    template <typename T, typename U>
    [[nodiscard]] constexpr bool operator!=(const MiAllocator<T>&, const MiAllocator<U>&) noexcept {
        return false;
    }

    /**
     * @if RU
     * @brief Вызывает деструктор объекта и освобождает его память через MiAllocator.
     * @tparam T Тип объекта.
     * @param ptr Указатель на живой объект; nullptr игнорируется.
     * @endif
     * @if EN
     * @brief Calls the object's destructor and frees its memory using MiAllocator.
     * @tparam T Object type.
     * @param ptr Pointer to a live object; `nullptr` is ignored.
     * @endif
     */
    template <typename T>
    struct MiDeleter {
        /// @if RU
        /// Разрушает объект и деаллоцирует занимаемый им блок.
        /// @endif
        /// @if EN
        /// Destroys the object and deallocates its memory block.
        /// @endif
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
     * @if RU
     * @brief Уникальный владеющий указатель с MiDeleter.
     * @tparam T Тип объекта.
     * @endif
     * @if EN
     * @brief Unique owning pointer with MiDeleter.
     * @tparam T Object type.
     * @endif
     */
    template <typename T>
    using MiUniquePtr = std::unique_ptr<T, MiDeleter<T>>;

    /**
     * @if RU
     * @brief Создаёт объект T без бросающих исключений через MiAllocator.
     * @tparam T Тип создаваемого объекта.
     * @tparam Args Типы аргументов конструктора.
     * @param args Аргументы конструктора, передаваемые с perfect forwarding.
     * @return Уникальный указатель либо пустой указатель при отказе аллокации.
     * @note Компиляция требует noexcept-конструктора T с заданными Args.
     * @endif
     * @if EN
     * @brief Constructs an object of type T without throwing, using MiAllocator.
     * @tparam T Type of the object to construct.
     * @tparam Args Types of the constructor arguments.
     * @param args Constructor arguments, passed using perfect forwarding.
     * @return Unique pointer, or an empty pointer if allocation fails.
     * @note Compilation requires T to have a noexcept constructor for the given Args.
     * @endif
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
