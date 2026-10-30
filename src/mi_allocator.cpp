#include <async/mi_allocator.hpp>
#include <mimalloc.h>
#include <mimalloc-new-delete.h>
#include <utility>

namespace shuttle::async {
    MiAllocationStats* MiAllocatorBase::exchangeAllocationCounter(
        MiAllocationStats* stats) noexcept {
        return std::exchange(m_allocationStats, stats);
    }

    void* MiAllocatorBase::operator new(std::size_t size) noexcept {
        return mi_malloc(size);
    }

    void MiAllocatorBase::operator delete(void* ptr) noexcept {
        mi_free(ptr);
    }

    void* MiAllocatorBase::allocate_bytes(std::size_t bytes, std::size_t alignment) noexcept {
        // Use mi_malloc_aligned to ensure that heavy graphics structures
        // (e.g., transformation matrices) align strictly with CPU cache line boundaries.
        void* ptr = mi_malloc_aligned(bytes, alignment);
        if (ptr != nullptr && m_allocationStats != nullptr) {
            ++m_allocationStats->allocations;
            m_allocationStats->bytes += bytes;
        }

        // Since exceptions are disabled, we deterministically return nullptr
        // instead of throwing std::bad_alloc in the event of an Out-Of-Memory (OOM) situation.
        return ptr;
    }

    void MiAllocatorBase::deallocate_bytes(void* ptr) noexcept {
        // mimalloc will atomically determine which thread and memory page this address belongs to.
        mi_free(ptr);
    }

    void* allocateBytes(std::size_t bytes, std::size_t alignment) {
        void* ptr = mi_malloc_aligned(bytes, alignment);

        return ptr;
    }

    void deallocateBytes(void *ptr) {
        mi_free(ptr);
    }
} // namespace shuttle::async
