# shuttle_async

**C++23 coroutine task and executor library by Eugenii Shagunov.**

**Documentation / Документация:** [English](README.en.md) | [Русский](README.ru.md)

This experimental project provides `AsyncTask<T>`, `BlockingTask<T>`, thread-pool and single-thread executors, structured coroutine scopes, cooperative cancellation, and allocator-aware shared state.

В этом экспериментальном проекте есть `AsyncTask<T>`, `BlockingTask<T>`, пул потоков и однопоточный исполнитель, структурированные coroutine scope, кооперативная отмена и общее состояние с пользовательским аллокатором.

Win32 asynchronous I/O is planned, not implemented. / Асинхронный ввод-вывод Win32 запланирован, но пока не реализован.

See the language-specific guides for build instructions, usage, testing, architecture, and roadmap:

- [English project guide](README.en.md)
- [Руководство на русском](README.ru.md)

API reference:

- English: [`docs/api/en/html/index.html`]
- Русский: [`docs/api/ru/html/index.html`]

PlantUML sources are in [`uml/`](uml/README.md). The original MIT license is in [`LICENSE`](LICENSE).
