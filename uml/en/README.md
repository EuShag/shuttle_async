# English PlantUML diagrams

Each `.puml` source in this directory contains English labels only. Russian
sources with matching filenames are in `../ru/`.

## Render PNG

Install Java and PlantUML, then configure the project with the path to the
PlantUML JAR:

```powershell
cmake -S . -B build -DPLANTUML_JAR="C:\tools\plantuml.jar"
cmake --build build --target uml_png
```

If CMake does not find Java automatically, also pass
`-DJAVA_EXECUTABLE="C:\path\to\java.exe"`. A PlantUML CLI executable can be
used instead with `-DPLANTUML_EXECUTABLE="C:\path\to\plantuml.exe"`.

PNG output is generated under `build/uml/en/`; Russian output is under
`build/uml/ru/`. Diagram sources are UTF-8.

## Diagram index

| Source | Diagram |
| --- | --- |
| `00_module_overview.puml` | Components and dependencies |
| `01_class_model.puml` | Public type relationships and ownership |
| `02_thread_pool_worker.puml` | Thread-pool scheduling and shutdown |
| `03_one_thread_worker.puml` | Single-thread executor lifecycle |
| `04_task_launch_and_completion.puml` | Task launch modes and completion |
| `05_shared_state_race.puml` | Shared-state waiter/completion race |
| `06_cancellation.puml` | Cooperative cancellation |
| `07_blocking_task_get.puml` | Blocking task wait |
| `08_scope_join.puml` | Scope child lifetime and joining |
| `09_errors_and_failures.puml` | Errors and failure behavior |
| `10_shared_state_ownership.puml` | Dynamic shared state allocation |
| `11_task_launch_activity.puml` | Task launch activity |
| `12_thread_pool_dispatch_activity.puml` | Worker dispatch activity |
| `coroutines.puml` | Shared-state lifecycle |
| `thread_pool_execute_sequence.puml` | Thread-pool task submission |
| `workers_sequence.puml` | Worker dispatch sequence |

Win32 asynchronous I/O is planned and is not present in these diagrams as an
implemented component.
