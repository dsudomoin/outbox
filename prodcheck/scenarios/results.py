#!/usr/bin/env python3
"""results.py [TEXT]: RESULTS.md from build/results, by the README section each scenario checks; TEXT says which code ran."""
import json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
GROUPS = [
    ("Гарантии: падения и остановка инстансов", [
        ("steady", "Три инстанса без сбоев, откаты 5 %: без потерь и без единого повтора; `send` вне транзакции и опечатка в топике падают до записи"),
        ("kill-one", "`kill -9` одного инстанса посреди батча, три раза"),
        ("kill-all-but-one", "`kill -9` всех, кроме одного: выживший доставляет за всех"),
        ("kill-all", "`kill -9` всех сразу, затем старт"),
        ("freeze-instance", "`SIGSTOP` на 50 с, как долгая пауза GC или замёрзшая VM; другие забирают его шарды"),
        ("rolling-restart", "Rolling-рестарт под нагрузкой: ни одного повтора, остановка не дольше `shutdownTimeout` + 8 с"),
        ("restart-past-deadline", "Батч длиннее `shutdownTimeout`: прерван, доставлен снова"),
        ("concurrent-start", "Три инстанса одновременно на пустой базе"),
    ]),
    ("Rolling-деплои, версии контрактов и откат", [
        ("rolling-upgrade", "Выкатка N-1 → N → N+1 под записью: ни отказов, ни ожидания"),
        ("skip-release", "N+1 рядом с N-1: запись v2 отвергается до записи"),
        ("rollback-to-transition", "Откат N+1 → N: N читает записанное, ничего не ждёт"),
        ("rollback-past-transition", "Откат за переходный релиз: v2 ждут, другие топики идут, докатка доставляет"),
        ("rollback-for-good", "Окончательный откат: `waiting/abandon`, затем `dead-letters/redrive`"),
        ("route-canary", "Новый маршрут рядом с релизом без него: запись отвергается, после выкатки проходит"),
        ("route-rollback", "Откат за релиз с маршрутом: записи ждут, вернувшийся релиз доставляет"),
        ("library-rollback", "Смешанный парк и откат на предыдущую версию библиотеки и обратно"),
    ]),
    ("Postgres и сеть до него", [
        ("pg-restart", "`docker restart` и падение сервера (`pg_ctl stop -m immediate`)"),
        ("pg-pause", "`docker pause` на 20 с; остановка инстанса в это время ограничена"),
        ("pg-conn-kill", "`pg_terminate_backend` всех соединений приложений, пять раз"),
        ("pg-blackhole", "Сеть молчит 25 с и возвращается, с `socketTimeout`"),
        ("pg-silent-drop", "Соединения молча обрываются навсегда, с `socketTimeout`; обслуживание тоже возвращается"),
        ("pg-silent-drop-no-timeout", "То же при `socketTimeout = 0`: воркеры выходят по `outbox.networkTimeout`"),
        ("pg-conn-exhausted", "Чужой клиент занимает все `max_connections` на 20 с"),
        ("pg-failover-sync", "Настоящая синхронная реплика, основная падает под нагрузкой, реплику повышают, приложения сами находят новую основную: сверка строгая"),
        ("pg-failover-async", "То же с асинхронной отстающей репликой: коммиты потерянного хвоста исчезают, у цели фантомы только от них, путь регрессии xid не срабатывает"),
        ("pg-version", "Образ и compose на Postgres 17: `PG_VERSION`, libfaketime, схема `accounts`"),
        ("restore-point", "Физическое восстановление на более раннюю точку (исход PITR и failover на отставшую реплику): фантомы только из потерянных коммитов"),
    ]),
    ("Пулер соединений", [
        ("pgbouncer-session", "pgbouncer в сессионном режиме, всё через него: как без пулера, gauge 0"),
        ("pgbouncer-txn", "Пул приложения через transaction pooling, соединение владельца напрямую: сверка строгая, обслуживание идёт, gauge 0"),
        ("pgbouncer-txn-all", "Всё через transaction pooling: старт отвергнут на занятом пуле, на спокойном проходит; обслуживание пропускает запуски с ERROR и gauge 1, доставка идёт"),
    ]),
    ("Фоновые потоки и `drain`", [
        ("route-unchecked", "Heartbeat не читает отчёты дольше 3 × `maintenanceInterval`: счётчик `route_unchecked`, WARN, отправки идут, восстанавливается сам"),
        ("drain", "`OutboxRelay.drain()`: relay выключен и включён, отложенные и ждущие записи, письма хука, записанная остановка, цель лежит"),
        ("drain-silent-drop", "`drain` при `socketTimeout = 0`, сеть молча теряет соединение: в шаге, в `checkHalt`, в проходе dead letters"),
        ("silent-drop-background", "`socketTimeout = 0`: соединения обслуживания, прохода dead letters и heartbeat молча потеряны посреди запроса; каждый поток возвращается сам"),
    ]),
    ("Ресурсы", [
        ("small-pool", "Пул по формуле README, обработчик с транзакцией в том же пуле"),
        ("pool-undersized", "Пул меньше формулы: медленно, но сходится само"),
        ("lock-table-full", "`sendAll` сверх таблицы блокировок: отказы писателей с 53200, relay переживает"),
        ("long-tx", "Транзакция с `send` открыта 40 с, читатель со старым снапшотом: остальные идут вовремя"),
    ]),
    ("Обработка сбоев: обработчик", [
        ("slow-handler", "Вызов дольше 80 % `batchTimeout`: сворачивание, ничего не потеряно"),
        ("failing-key", "Сбоит один ключ: он в retry, остальные с прежней задержкой, порядок цел"),
        ("flaky", "30 % вызовов падают"),
        ("poison", "Ядовитое сообщение: dead с ERROR и метрикой, ключ и шард идут дальше"),
        ("max-attempts", "`maxAttempts` исчерпан: dead, затем `dead-letters/redrive`"),
        ("stuck-head-fix", "`maxAttempts = 0`, ключ сбоит вечно: rolling-рестарт с `maxAttempts`, затем `redrive`"),
        ("hang-one-instance", "Вызов игнорирует прерывание и занимает потоки: другой инстанс забирает шарды"),
        ("hang-everywhere", "То же на всех инстансах: доставка стоит, вызовы вернулись, пошла сама"),
        ("routes-isolation", "Сбоит один маршрут из трёх: остальные доставляют в тех же вызовах"),
        ("bad-batch", "`handleBatch` бросает исключение, `Error`, возвращает null, не тот размер, null в списке"),
        ("requeue", "`Outcome.requeue` и `kill -9`: ключ идёт, запись возвращается, копий нет"),
        ("requeue-limit", "`maxRequeues`: dead с «requeue limit reached»"),
        ("retry-attempts", "`Outcome.retry`, затем доставка: порядок ключа цел"),
        ("wait-handler", "`Outcome.waitFor`: попытки не тратятся, алерт есть, порядок цел"),
        ("typed-garbage", "Типизированный маршрут: нечитаемый payload и tombstone сразу в dead"),
        ("retry-long-tail", "У одного ключа больше 100 000 строк в retry и столько же в parked: остальные ключи с прежней задержкой, планы запросов отдельно, порядок цел"),
    ]),
    ("Недоступность цели", [
        ("target-down", "`kill -9` цели на 30 с: запись и другие маршруты идут, в dead ничего"),
        ("target-long-outage", "Цель лежит 90 с: возвращается сама, не дольше `maxDelay`"),
        ("target-db-down", "Лежит база цели"),
        ("lost-response", "Эффект сделан, ответ потерян: повторное прибытие, эффект один"),
        ("late-request", "Запрос переживает таймаут клиента и приходит после повтора"),
        ("target-net", "Соединение с целью рвётся и молчит"),
    ]),
    ("Dead letters и админ-API", [
        ("admin", "Все операции по HTTP на живом инстансе под нагрузкой: доступ, ввод, страницы, discard, redrive, аудит"),
        ("hook", "Все решения хука, его сбои и зависание, `kill -9`; оставленные письма: `redrive`"),
        ("hook-mixed-fleet", "Парк с хуком и без: письма ждут хук, пока он жив"),
        ("dead-retention", "`retention` удаляет оставленные письма, `pending` не трогает"),
    ]),
    ("Партиции, хвост и сироты", [
        ("rotation", "Крошечные диапазоны и `kill -9` посреди DDL: диапазоны создаются и удаляются"),
        ("tail-overflow", "Держатель блокировки: строки в хвосте, ни одна запись не падает, хвост становится диапазоном"),
        ("tail-big", "Хвост больше `maxTailScan` остаётся горячим до окна `tailConversion`"),
        ("tail-detached", "Хвост отсоединён вручную: обслуживание возвращает"),
        ("orphan-foreign", "Диапазон со строками отсоединён вручную: `orphans/{table}/restore` на живом инстансе"),
        ("orphan-own", "Падение между detach и drop: строки возвращаются сами"),
        ("inbox-orphan", "Отсоединённый диапазон inbox присоединяется обратно, двойного эффекта нет"),
        ("inbox-rotation", "Диапазоны inbox создаются и удаляются под нагрузкой"),
        ("ddl-lock", "Чужой `ACCESS EXCLUSIVE` на партиции 12 с"),
        ("relay-off", "Ни на одном инстансе нет relay: запись идёт, rolling-рестарт доставляет всё"),
        ("stranded", "Тик шарда удалён: `recovery/stranded-shards` на живом инстансе"),
    ]),
    ("Регрессия xid", [
        ("xid-auto", "`pg_dump` и `pg_restore` в новый кластер под живыми инстансами: восстанавливается само"),
        ("xid-halt", "Режим `halt`: relay стоит до одного вызова админ-API"),
        ("xid-max-rows", "Строк больше `maxRows`: то же"),
    ]),
    ("Часы", [
        ("db-clock", "Часы базы прыгают вперёд и назад на минуту под нагрузкой, ключ в retry"),
        ("app-clock", "Инстанс с часами +2 ч, -2 ч, +3 дня"),
        ("app-clock-retry", "Инстанс с часами +2 ч ставит сроки и уходит: остальные не ждут его часов"),
        ("inbox-clock-sender", "Часы отправителя ±2 ч и ±3 дня, цель дедуплицирует через inbox: ни второго эффекта, ни потерь, ни 23514"),
        ("inbox-clock-receiver", "Часы получателя ±2 ч и ±3 дня: то же"),
    ]),
    ("Несколько outbox, приложений и шардов", [
        ("two-outboxes", "Два outbox в одном процессе: зависание одного не трогает другой"),
        ("two-apps-one-db", "Два приложения со своими таблицами в одной базе и `kill -9`"),
        ("shards-mismatch", "Инстанс с другим `outbox.shards` не стартует, остальные не замечают"),
        ("reshard", "Запланированный решардинг по процедуре README, со всеми остановленными"),
        ("reshard-stale", "Замороженный инстанс проспал решардинг: его relay стоит, запись отвергается до рестарта"),
        ("everything", "Всё сразу: два приложения, откаты, poison, requeue, сбойная цель, убийства, долгая транзакция"),
    ]),
    ("Очередь по счёту: Postgres 17, миграции и две роли, `handleBatch` с `requeue`, блокировка строки в обработчике, один воркер", [
        ("accounts-start-check", "DDL миграции принят; без `SELECT` на последовательность старт падает и называет право"),
        ("accounts-kill", "`kill -9` посреди вызова с отложенными записями"),
        ("accounts-conn-kill", "`pg_terminate_backend`: бизнес-коммит прошёл, коммит шага нет"),
        ("accounts-row-lock", "Строка счёта заблокирована 70 с, `socketTimeout` равен `batchTimeout`"),
        ("accounts-row-lock-fixed", "То же с `lock_timeout` 2 с в бизнес-транзакции"),
        ("accounts-zombie", "Ожидание JDBC переживает `batchTimeout`: брошенный вызов коммитит после начала повтора, эффект один"),
        ("accounts-corrupt", "Сохранённый конверт не разбирается: ключ стоит, rolling-рестарт с `maxAttempts`"),
        ("accounts-requeue-storm", "Тысячи отложенных записей при `maxRequeues = 0`: цена для других счетов"),
        ("accounts-requeue-storm-delay", "То же с `relay.requeue.delay = 5s`: повторные прибытия и задержка других счетов рядом с 1 с"),
        ("accounts-pool-pressure", "Писатели занимают весь пул из 10"),
        ("accounts-owner-down", "Роль-владелец не подключается 30 с: доставка идёт, обслуживание догоняет"),
        ("accounts-rolling", "Rolling-рестарт с отложенными записями"),
        ("accounts-pg-restart", "Рестарт Postgres и минута без него при `maxDelay` по умолчанию"),
        ("accounts-same-type-held", "`kill -9` с правилом README в обработчике: отложенные записи ключа строго по порядку"),
        ("accounts-held-deadlock", "Правило README о записях того же типа: запись с ожидаемым результатом того же типа удержана навсегда, ключ стоит; это и проверяется"),
        ("accounts-unknown-source-ack", "Неизвестный `source` как в сервисе: событие подтверждено и исчезло без эффекта, это и проверяется"),
        ("accounts-unknown-source-wait", "Рекомендация: `Outcome.waitFor`, попытки не тратятся, другие счета идут, релиз с новым `source` доставляет всё"),
        ("accounts-corrupt-fixed", "Рекомендация: нечитаемый конверт сразу в dead, счёт идёт дальше, письмо видно, `redrive` и `discard` работают"),
        ("accounts-recommended", "Все рекомендации вместе: `kill -9`, блокировка строки, нечитаемый конверт, неизвестный `source`, рестарт Postgres; порядок внутри типа, другие счета, метрики и `/status`"),
        ("accounts-rollback-drain", "Откат сервиса по-настоящему: приём остановлен, очередь удаляют только при пустом `/status`; ждущие через `waiting/abandon`, вечно отложенные через `maxRequeues`"),
    ]),
]

out = ["# Итог проверки без Kafka", ""]
if len(sys.argv) > 1:
    out += [f"{sys.argv[1]} Как устроена сверка и как запустить заново: [README](README.md).", ""]
out += ["«Повторные прибытия» считают все приходы сверх первого, и попытки, которые цель отвергла, и ответы `requeue`: это не "
        "двойной эффект, двойных эффектов сверка не допускает. Задержка: от коммита до эффекта, p99 и максимум, с.", ""]
total = passed = 0
named = set()
for title, rows in GROUPS:
    out += [f"## {title}", "", "| Сценарий | Что проверяет | Итог | Как лечится | Отправлено | Потеряно | Повторные прибытия | Задержка |",
            "|---|---|---|---|---|---|---|---|"]
    for name, text in rows:
        named.add(name)
        path = f"{HERE}/../build/results/{name}.json"
        if not os.path.exists(path):
            out.append(f"| `{name}` | {text} | не запускался | | | | | |")
            continue
        d = json.load(open(path))
        led = d["ledger"]
        total += 1
        passed += d["verdict"] == "PASS"
        verdict = "да" if d["verdict"] == "PASS" else f"**{d['verdict']}**"
        out.append(f"| `{name}` | {text} | {verdict} | {d.get('repaired') or ''} | {led.get('sent', '')} | {led.get('lost', '')} | "
                   f"{led.get('duplicates', '')} | {led.get('latency_p99', '')} / {led.get('latency_max', '')} |")
    out.append("")
out.insert(4, f"Прошло {passed} из {total}.\n")
missing = {f[:-5] for f in os.listdir(f"{HERE}/../build/results") if f.endswith(".json")} - named
if missing:
    print("not in the table:", sorted(missing), file=sys.stderr)
open(f"{HERE}/../RESULTS.md", "w").write("\n".join(out))
print(f"{passed}/{total}")
