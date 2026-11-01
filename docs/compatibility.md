# Совместимость кандидата 0.1.0

Поддерживаемый кодом диапазон Python: 3.11–3.14. Локальная проверка выполнена
на CPython 3.14.7, macOS arm64. CI содержит матрицу 3.11–3.14 на Linux;
наличие workflow ещё не означает, что эта матрица прошла.

Зависимости приложения необязательны. Основной runtime и sync facade используют
только stdlib. Код расширений импортирует Kafka, Protobuf и Allure лениво.
Пассивный pytest plugin не открывает соединения и не изменяет asyncio mode.

Конкретное development-окружение закреплено в `uv.lock`. Объявленные нижние
границы extras пока являются кандидатными: полная проверка минимальных версий,
всех платформ и сочетаний generated-code/runtime остаётся release gate.
Не следует считать широкий диапазон в metadata результатом такой проверки.

Текущие локальные версии и статус реальных Kafka-тестов фиксируются в
[status.md](status.md). Дополнительные условия Kafka описаны в [kafka.md](kafka.md).

`max_bytes` — детерминированная оценка удерживаемых данных, не предел RSS.
Она включает длины raw payload/key/UTF-8 source, 128 байт envelope, по 32 байта
на header/metadata pair, кодированный размер metadata, оценку decoded payload
и 128 байт record. Оценка Protobuf основана на wire size с поправкой и не
приравнивается к размеру native heap. Native queues, snapshots и пользовательские
результаты учитываются отдельно от удерживаемого журнала.

Один активный sync watcher имеет один runtime thread; Kafka добавляет один
сериализованный polling worker. Произвольный блокирующий callback нельзя
принудительно прервать. При зависании cleanup фиксируется incomplete/abandoned
outcome; библиотека не утверждает, что принудительно остановила поток.

Temporal assertions, reconnect/checkpoint recovery и другие брокеры отложены.
Проверки `wait_for` не доказывают порядок, отсутствие будущих событий,
exactly-once или глобальный порядок Kafka partitions.
