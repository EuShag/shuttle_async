//
// Created by Shagu on 30.10.2026.
//

#ifndef SHUTTLE_ASYNC_INTRUSIVE_PTR_HPP
#define SHUTTLE_ASYNC_INTRUSIVE_PTR_HPP
#include <atomic>

namespace shuttle::async {
    template<typename T>
    class IntrusivePtr {
    public:
        // Конструктор захватывает владение сырым указателем
        explicit IntrusivePtr(T* ptr) : m_ptr(ptr) {}

        // Деструктор Владельца. Когда IntrusivePtr умирает, он запускает markDeletion
        ~IntrusivePtr() {
            if (m_ptr) {
                markDeletion();
            }
        }

        // Запрещаем копирование Владельца (он уникален, как std::unique_ptr)
        IntrusivePtr(const IntrusivePtr&) = delete;
        IntrusivePtr& operator=(const IntrusivePtr&) = delete;

        // Разрешаем перемещение владения
        IntrusivePtr(IntrusivePtr&& other) noexcept
            : m_ptr(std::exchange(other.m_ptr, nullptr)),
              m_refCount(other.m_refCount.load(std::memory_order_relaxed)) {}

        IntrusivePtr& operator=(IntrusivePtr&& other) noexcept {
            if (this == &other) return *this;
            if (m_ptr) markDeletion(); // Сначала уничтожаем свой старый ресурс
            m_ptr = std::exchange(other.m_ptr, nullptr);
            m_refCount.store(other.m_refCount.load(std::memory_order_relaxed), std::memory_order_relaxed);
            return *this;
        }

        // --- БЕЗОПАСНЫЙ ИНТЕРФЕЙС ВЗАИМОДЕЙСТВИЯ ---
        T* get() noexcept { return m_ptr; }
        const T* get() const noexcept { return m_ptr; }
        T& operator*() { return *m_ptr; }
        T* operator->() { return m_ptr; }
        explicit operator bool() const noexcept { return m_ptr != nullptr; }

        // =========================================================================
        // ВЛОЖЕННЫЙ КЛАСС: СИЛЬНАЯ ССЫЛКА ДЛЯ АСИНХРОННЫХ ЗАДАЧ (I/O)
        // =========================================================================
        class StrongRef {
        public:
            explicit StrongRef(IntrusivePtr* master) : m_master{master} {}

            ~StrongRef() {
                if (m_master) {
                    m_master->release(m_win32Instance);
                }
            }

            // Перед смертью ссылки в коллбэке Win32 прокидываем instance для Disassociate
            void setCallbackInstance(void* instance) noexcept {
                m_win32Instance = instance;
            }

            StrongRef(const StrongRef&) = delete;
            StrongRef& operator=(const StrongRef&) = delete;

            StrongRef(StrongRef&& other) noexcept
                : m_master{std::exchange(other.m_master, nullptr)},
                  m_win32Instance{std::exchange(other.m_win32Instance, nullptr)} {}

            StrongRef& operator=(StrongRef&& other) noexcept {
                if (this == &other) return *this;
                if (m_master) m_master->release(m_win32Instance);
                m_master = std::exchange(other.m_master, nullptr);
                m_win32Instance = std::exchange(other.m_win32Instance, nullptr);
                return *this;
            }

            [[nodiscard]] T& get() const noexcept { return *m_master->get(); }
            T* operator->() const noexcept { return m_master->get(); }
            T& operator*() const noexcept { return *m_master->get(); }

        private:
            IntrusivePtr* m_master;
            void* m_win32Instance = nullptr;
        };
        // =========================================================================

        // Фабрика сильных ссылок для пула потоков
        std::optional<StrongRef> tryGetStrongRef() {
            if (!tryAcquire()) return std::nullopt;
            return StrongRef{this};
        }

    private:
        static constexpr uint32_t DELETED_BIT = 1 << 31;
        static constexpr uint32_t COUNTER_MASK = ~DELETED_BIT;

        std::atomic<uint32_t> m_refCount{0};
        T* m_ptr{nullptr};

        bool tryAcquire() {
            auto current = m_refCount.load(std::memory_order_relaxed);
            while (true) {
                if (current & DELETED_BIT) {
                    return false;
                }
                if (m_refCount.compare_exchange_weak(current, current + 1,
                                                     std::memory_order_acq_rel,
                                                     std::memory_order_relaxed)) {
                    return true;
                }
            }
        }

        void release(void* win32_callback_instance = nullptr) {
            if (win32_callback_instance) {
                // Идеальное место для вызова Win32 API перед физическим декрементом!
                // DisassociateCurrentThreadFromCallback(static_cast<PTP_CALLBACK_INSTANCE>(win32_callback_instance));
            }

            auto prevValue = m_refCount.fetch_sub(1, std::memory_order_acq_rel);

            // Если владелец уже уничтожен (DELETED_BIT взведен) и мы были последней живой задачей
            if ((prevValue & DELETED_BIT) && (prevValue & COUNTER_MASK) == 1) {
                delete m_ptr;
                m_ptr = nullptr;
                // delete this; НЕЛЬЗЯ вызывать, так как класс IntrusivePtr может лежать на стеке!
            }
        }

        void markDeletion() {
            auto prevValue = m_refCount.fetch_or(DELETED_BIT, std::memory_order_acq_rel);

            // Если активных асинхронных задач (ссылок) на момент уничтожения владельца нет,
            // мы можем мгновенно удалить целевой объект.
            if ((prevValue & COUNTER_MASK) == 0) {
                delete m_ptr;
                m_ptr = nullptr;
            }
        }
    };

    template<typename T>
    using IntrusiveStrongRef = IntrusivePtr<T>::StrongRef;
}

#endif //SHUTTLE_ASYNC_INTRUSIVE_PTR_HPP
