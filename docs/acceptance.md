# Проверяемость SPEC.md

Таблица связывает реализованные области с воспроизводимыми проверками. Строка
не означает автоматического закрытия каждого acceptance criterion диапазона:
полная release traceability AC-01–AC-88 и RG-01 остаётся обязательной перед
публикацией. Текущий локальный результат приведён в [status.md](status.md).

| AC / область | Evidence в репозитории | Граница проверки |
| --- | --- | --- |
| AC-01–06: lazy factory, optional imports, sync/async, READY | `tests/test_api.py`, `tests/test_observer.py`, `tests/test_contracts.py` | Базовые сценарии и import isolation проверены локально. |
| AC-07–19: Kafka positions, transactions, topology, isolation | `tests/test_kafka.py`, `tests/integration/` | Native doubles отделены от real-broker nodes; полный Kafka fault profile ещё нужен. |
| AC-20–21: raw null/empty, duplicate/null headers | `tests/test_api.py`, transport contracts | Все поля сохраняются detached и immutable. |
| AC-22–27: Protobuf, required fields, mixed types | `tests/test_decoders.py`, `tests/test_protobuf_runtime.py` | Generated wrapper types и synthetic descriptors; полный runtime/gencode version matrix ещё нужен. |
| AC-28–29: mutation isolation | `tests/test_matchers.py`, `tests/test_observer.py`, `tests/test_protobuf_runtime.py` | Predicate, result, snapshot и branches не изменяют канонический payload. |
| AC-30–35: retention, hard limit, loss ledger, budgets | `tests/test_observer.py`, `tests/test_reliability.py`, `tests/test_settlement.py` | Консервативная полнота и отдельный результат discard; RSS benchmarks ещё нужны. |
| AC-36–44: history/count, callbacks, tri-state, presence | `tests/test_observer.py`, `tests/test_matchers.py` | Truth tables, неверные callback results, configuration errors. |
| AC-45–49: immutable scopes, nesting, from-now, snapshots | `tests/test_observer.py`, `tests/test_reliability.py`, `tests/test_reporting.py` | Границы scopes не расширяют родителя; snapshot не вызывает predicates повторно. |
| AC-50–53: deadline/commit priority | `tests/test_reliability.py` | Injected clock: до, на и после deadline; error-before-commit и callback overrun. |
| AC-54–59: background failure, cancellation, close | `tests/test_observer.py`, `tests/test_reliability.py`, `tests/test_kafka.py` | Startup rollback, concurrent close, primary error priority, full handoff. Непрерываемые native hangs требуют subprocess fault profile. |
| AC-60–68: sync facade | `tests/test_observer.py`, `tests/test_reliability.py`, `tests/test_kafka.py` | Blocking action collection, value returns, affinity, reentrancy, 100 lifecycle cycles; полный Ctrl-C subprocess profile ещё нужен. |
| AC-69–75: diagnostics/redaction/Allure/trace | `tests/test_diagnostics.py`, `tests/test_reporting.py` | Safe defaults, named bounded exports, failed redactor, reporter thread/context, W3C extraction. Полная Allure runner matrix ещё нужна. |
| AC-76–78: adapter contracts/fault applicability | `testing/contracts`, `testing/harness.py`, `testing/kafka.py`, `tests/integration/test_kafka_contracts.py` | Общие nodes применяются без изменения Core. Toxiproxy routing ещё нужен. |
| AC-79–83: packaging, passive pytest, xdist | `.github/workflows/ci.yml`, `tests/test_api.py`, чистые установки candidate artifacts | Wheel/sdist, `py.typed`, stdlib-only import и pure-sync pytest проверены отдельно. |
| AC-84: bounded runtime/resources | Journal, bridge, diagnostic/export budgets; reliability suites | Удерживаемые собственные структуры ограничены; полный load/RSS benchmark ещё нужен. |
| AC-85–88: artifact integrity, publishing, docs | `pyproject.toml`, `uv.lock`, CI, docs/status.md | Build и metadata checks выполнены. Имя/лицензия/Trusted Publisher/attestations требуют владельца; публикации не было. |

Результаты fake/native-double tests не выдаются за доказательство брокерных
гарантий. Явный skip при отсутствии инфраструктуры сохраняет этот пробел видимым.
Измеренная скорость выполнения этих тестов не является performance baseline
production-библиотеки.
