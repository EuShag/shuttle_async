//
// Created by Shagu on 12.10.2026.
//

#ifndef SHUTTLEENGINE_LAUNCHMODE_HPP
#define SHUTTLEENGINE_LAUNCHMODE_HPP

#endif //SHUTTLEENGINE_LAUNCHMODE_HPP

namespace shuttle::async {
    class IExecutor;

    /**
     * @if RU
     * @brief Режим запуска дочерней корутины через CoroScope.
     * @endif
     * @if EN
     * @brief Launch mode for a child coroutine created through CoroScope.
     * @endif
     */
    enum class LaunchMode {
        /// @if RU
        /// Запустить корутину сразу в текущем потоке.
        /// @endif
        /// @if EN
        /// Run the coroutine immediately on the current thread.
        /// @endif
        Immediate,
        /// @if RU
        /// Поставить корутину в заданный исполнитель до возврата из launch.
        /// @endif
        /// @if EN
        /// Enqueue the coroutine on the specified executor before `launch` returns.
        /// @endif
        Async,
        /// @if RU
        /// Вернуть созданную корутину, не запуская её; запуск выполняется позднее.
        /// @endif
        /// @if EN
        /// Return the created coroutine without starting it; it is started later.
        /// @endif
        Lazy
    };

    /**
     * @if RU
     * @brief Настройки запуска дочерней задачи.
     *
     * Указатель исполнителя не владеющий: он должен оставаться действительным
     * в течение постановки и исполнения задачи. Для `Immediate`/`Lazy` он
     * не используется самим запуском; для `Async` требуется ненулевой указатель.
     * @endif
     * @if EN
     * @brief Launch settings for a child task.
     *
     * The executor pointer is non-owning and must remain valid while the task is
     * enqueued and executed. Launch itself does not use it for `Immediate` or
     * `Lazy`; `Async` requires a non-null pointer.
     * @endif
     */
    struct LaunchOptions {
        /// @if RU
        /// Выбранный режим; по умолчанию задача запускается асинхронно.
        /// @endif
        /// @if EN
        /// Selected mode; tasks launch asynchronously by default.
        /// @endif
        LaunchMode mode{ LaunchMode::Async };
        /// @if RU
        /// Исполнитель для режима `Async`; nullptr приводит к ошибке запуска.
        /// @endif
        /// @if EN
        /// Executor for `Async` mode; `nullptr` results in a launch error.
        /// @endif
        IExecutor* explicitExecutor{ nullptr };
    };
} // namespace shuttle::async
