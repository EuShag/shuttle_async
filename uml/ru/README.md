# Диаграммы PlantUML на русском языке

Все файлы `.puml` в этой директории содержат подписи только на русском.
Английские версии с такими же именами находятся в `../en/`.

## Генерация PNG

Установите Java и PlantUML, затем настройте проект, указав путь к JAR-файлу
PlantUML:

```powershell
cmake -S . -B build -DPLANTUML_JAR="C:\tools\plantuml.jar"
cmake --build build --target uml_png
```

Если CMake не обнаружил Java автоматически, укажите также
`-DJAVA_EXECUTABLE="C:\путь\к\java.exe"`. Вместо JAR можно использовать
исполняемый файл PlantUML: `-DPLANTUML_EXECUTABLE="C:\путь\к\plantuml.exe"`.

PNG создаются в `build/uml/ru/`, а английские PNG — в `build/uml/en/`.
Исходники диаграмм имеют кодировку UTF-8.

## Список диаграмм

| Исходник | Диаграмма |
| --- | --- |
| `00_module_overview.puml` | Компоненты и зависимости |
| `01_class_model.puml` | Связи типов и владение |
| `02_thread_pool_worker.puml` | Планирование и остановка пула |
| `03_one_thread_worker.puml` | Жизненный цикл однопоточного исполнителя |
| `04_task_launch_and_completion.puml` | Запуск и завершение задач |
| `05_shared_state_race.puml` | Гонка регистрации ожидания и завершения |
| `06_cancellation.puml` | Кооперативная отмена |
| `07_blocking_task_get.puml` | Блокирующее ожидание задачи |
| `08_scope_join.puml` | Время жизни дочерних задач и join |
| `09_errors_and_failures.puml` | Ошибки и аварийные пути |
| `10_shared_state_ownership.puml` | Динамическое общее состояние |
| `11_task_launch_activity.puml` | Activity-диаграмма запуска задачи |
| `12_thread_pool_dispatch_activity.puml` | Activity-диаграмма планирования |
| `coroutines.puml` | Жизненный цикл общего состояния |
| `thread_pool_execute_sequence.puml` | Отправка задачи в пул потоков |
| `workers_sequence.puml` | Последовательность работы worker-а |

Асинхронный Win32 I/O запланирован, но в текущей реализации отсутствует.
