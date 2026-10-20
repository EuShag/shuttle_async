# PlantUML Architecture Diagrams

Each `.puml` source file in this directory contains English labels and technical documentation only. Diagram sources are encoded in UTF-8.

## Render PNG Diagrams via CMake

To generate high-resolution PNG diagrams from the `.puml` sources, ensure you have **Java** and the **PlantUML** JAR library installed on your system. Configure and run the dedicated CMake target as follows:

```powershell
cmake -S . -B build -DPLANTUML_JAR="C:\tools\plantuml.jar"
cmake --build build --target uml_png
```

### Advanced Configuration Notes
* **Custom Java Path:** If CMake fails to locate your Java installation automatically, explicitly provide the path using the `-DJAVA_EXECUTABLE="C:\path\to\java.exe"` flag.
* **PlantUML Executable:** Alternatively, if you are using a standalone PlantUML CLI binary instead of the JAR file, pass it via `-DPLANTUML_EXECUTABLE="C:\path\to\plantuml.exe"`.

All generated PNG files are deterministically output to the `build/uml/` directory.

## Architecture Diagram Index

The repository includes component, class, state, activity, and sequence diagrams mapping out the complete lock-free runtime lifecycle:

| Source File | Diagram Type & Description |
| :--- | :--- |
| `00_module_overview.puml` | **Components Overview:** Internal module structures and core system dependencies |
| `01_class_model.puml` | **Class Model:** Public type relationships, API contracts, and object ownership |
| `02_thread_pool_worker.puml` | **ThreadPool Lifecycle:** Worker thread scheduling loop and graceful shutdown mechanics |
| `03_one_thread_worker.puml` | **Single-Thread Executor:** Lifecycle and task processing of the `OneThreadExecutor` |
| `04_task_launch_and_completion.puml` | **Task Management:** Launch strategies (`Async`, `Lazy`, `Immediate`) and completion flows |
| `05_shared_state_race.puml` | **Race Coordination:** Lock-free state machine handling waiter registration vs completion races |
| `06_cancellation.puml` | **Cancellation Flow:** Cooperative task cancellation graph and cancellation token paths |
| `07_blocking_task_get.puml` | **Synchronous Wait:** Blocking task execution via `get()` and kernel-level thread suspension |
| `08_scope_join.puml` | **Structured Concurrency:** Scope child lifetime tracking, `joinTask` injection, and group joining |
| `09_errors_and_failures.puml` | **Error Handling:** System failure propagation, memory constraints, and panic behaviors |
| `10_shared_state_ownership.puml` | **Memory Management:** Dynamic allocation and reference-counted lifecycle of `AsyncSharedState` |
| `11_task_launch_activity.puml` | **Launch Activity:** Step-by-step activity diagram mapping out coroutine activation |
| `12_thread_pool_dispatch_activity.puml` | **Worker Dispatch Activity:** Lock-free work-stealing and queue scanning activity loop |
| `coroutines.puml` | **State Machine:** Comprehensive lifecycle state transitions of C++23 coroutine frames |
| `thread_pool_execute_sequence.puml` | **Submission Sequence:** High-performance task injection into local deques or global queues |
| `workers_sequence.puml` | **Work-Stealing Sequence:** Detailed interaction timeline between concurrent execution threads |

*Note: Win32 asynchronous I/O (IOCP / Overlapped) is currently planned for the upcoming milestone and is not yet represented in these architectural diagrams.*
