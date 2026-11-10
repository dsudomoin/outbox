# prodcheck

Тестовые приложения для проверки готовности библиотеки к продакшену. Отдельная сборка: не входит в корневой
`settings.gradle.kts` и берёт библиотеку из `mavenLocal()` как опубликованный артефакт, только через публичный API.

| Модуль | Что это |
|---|---|
| `common` | Java без процессоров: генератор нагрузки, записи в журнал (`pc_*`), управление обработчиками |
| `target` | Java: HTTP-цель со своей базой и `InboxModule`, неисправности задаются через `PUT /fault` |
| `shop` | Java: только `OutboxTopicHandler` (`payments`, `ledger`, `projection`, `orders`, `audit`), три релиза контракта |
| `accounts` | Kotlin 2.1 + KSP: outbox как долговечная очередь входных событий по счёту: своя схема, роль приложения и роль-владелец, миграция вместо bootstrap, `ProtobufOutbox`, `handleBatch` с `requeue`, бизнес-транзакция с блокировкой строки счёта в обработчике, один воркер, значения библиотеки по умолчанию, без метрик и админки |
| `courier` | Kotlin + KSP: `OutboxHandler` с `handleBatch` на все топики, маршрут `routed`, второй outbox в том же процессе |
| `store` | Java, Spring Boot 3.5 или 4.1: двойник `shop-n` на адаптере Spring. Запись в транзакции JPA (строка заказа через Hibernate, журнал и `send` в ней же), `projection` — обработчик с `@Transactional` и inbox, `ledger` — на втором DataSource, админ-API на Spring MVC, метрики и readiness через Actuator. Те же топики и контракты, что у `shop-n`, поэтому оба работают на одних таблицах |

## Сборка

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
./gradlew publishToMavenLocal -x test -q                  # библиотека этого checkout
./gradlew -p prodcheck installDist -Prelease=n            # target, courier и shop-n
./gradlew -p prodcheck :shop:installDist -Prelease=n1     # shop-n1 рядом
./gradlew -p prodcheck :shop:installDist -Prelease=n2     # shop-n2 рядом
./gradlew -p prodcheck :store:installDist -PbootLine=4.1  # store-boot41
./gradlew -p prodcheck :store:installDist -PbootLine=3.5  # store-boot35 рядом
```

`POST /tx/send?raw=garbage|null` пишет мимо `TypedOutbox` байты `not json` или tombstone, со строкой в `pc_sent`. Нагрузка
своя у каждого outbox: `POST /load?outbox=second` не трогает `main`, `DELETE /load` останавливает все, а
`DELETE /load?outbox=second` только этот. `PUT /handler?waiting=true|false` управляет тегом `wait` у `courier`.

Релизы `shop` (`-Prelease=`): `n1` пишет и читает v1 и не знает `audit`; `n` пишет v1, читает v1 и v2, есть `audit`;
`n2` пишет v2. Другая версия библиотеки: `-PoutboxVersion=...`, установка ляжет в `shop-<release>-<версия>`.

## accounts

Схема создаётся заранее: `accounts/roles.sql` под суперпользователем, `accounts/schema.sql` под `accounts_owner`,
`accounts/grant-sequence.sql` под `accounts_owner`, `scenarios/ledger.sql` под `accounts_owner` с `search_path=accounts`.
Счета `k0..k29` добавьте в `accounts.subscription`. Собирается отдельно: `./gradlew -p prodcheck :accounts:installDist`.

## Запуск

```bash
docker compose -f prodcheck/docker-compose.yml up -d --build --wait
docker exec -i kora-outbox-prodcheck-source psql -U outbox -d outbox -q < prodcheck/scenarios/ledger.sql
docker exec -i kora-outbox-prodcheck-target psql -U outbox -d outbox -q < prodcheck/scenarios/ledger.sql

prodcheck/target/build/install/target/bin/target
INSTANCE_NAME=shop-1 prodcheck/shop/build/install/shop-n/bin/shop
INSTANCE_NAME=courier-1 JAVA_OPTS="-Doutbox.table=courier_main" prodcheck/courier/build/install/courier/bin/courier

docker compose -f prodcheck/docker-compose.yml down -v
```

Порты по умолчанию (публичный, метрики): `target` 9090, 9095; `shop` 9080, 9085; `courier` 9081, 9086. `shop` и
`courier` по умолчанию пишут в таблицу `outbox`: на одной базе их разводит `-Doutbox.table`, иначе у них общий outbox.

Окружение: `HTTP_PORT`, `METRICS_PORT`, `INSTANCE_NAME`, `POSTGRES_URL`, `TARGET_DB_URL`, `TARGET_URL`, `DB_POOL_SIZE`.

Любой ключ конфигурации задаётся при старте через `JAVA_OPTS`: системные свойства ложатся поверх `application.conf`,
длительности тоже, например `JAVA_OPTS="-Doutbox.relay.workers=2 -Doutbox.relay.batchTimeout=5s -DhttpClient.target.requestTimeout=1s"`.

Управление во время работы: `POST /load`, `DELETE /load`, `GET /stats`, `POST /tx/send`, `POST /tx/hold`,
`POST /tx/outside`, `PUT /handler`, `GET /handler`; у `shop` ещё `PUT /hook`, у `courier` ещё `GET /second/status`, у
`target` `PUT /fault`, `GET /fault`, `GET /health`. Админ-API: `/kora-outbox/admin/v1`, заголовок `X-Api-Key`, ключи
`prodcheck-admin` и `prodcheck-reader`.

## Сценарии

Матрица сбоев выведена из разделов README библиотеки: «Гарантии», «Сценарии сбоев», «Обработка сбоев», «Эксплуатация». Что в
ней есть и чем закончилось, в [RESULTS.md](RESULTS.md); на другом коде библиотеки его надо получить заново:

```bash
./gradlew publishToMavenLocal -x test -q
./gradlew -p prodcheck :shop:installDist -Prelease=n1 -q
./gradlew -p prodcheck :shop:installDist -Prelease=n2 -q
./gradlew -p prodcheck installDist -Prelease=n -q
# предыдущая версия библиотеки для library-rollback: git archive <rev>, в нём publishToMavenLocal с -Pversion=0.0.0-<rev>
./gradlew -p prodcheck :shop:installDist -Prelease=n -PoutboxVersion=0.0.0-<rev> -q
docker compose -f prodcheck/docker-compose.yml --profile tools build
prodcheck/scenarios/run-all.sh                  # вся матрица, около полутора часов
prodcheck/scenarios/run-all.sh kill-one admin   # отдельные сценарии
SHOP_BIN=$PWD/prodcheck/store/build/install/store-boot41/bin/store RESULT_DIR=$PWD/prodcheck/build/results-spring41 \
  prodcheck/scenarios/run-all.sh kill-one       # те же сценарии на Spring Boot вместо shop-n
python3 prodcheck/scenarios/summary.py          # итог по build/results (RESULT_DIR, PG_VERSION выбирают другой каталог)
prodcheck/scenarios/run-pg17.sh                 # accounts-*, pg-*, steady, kill-one, xid-auto, inbox-rotation, retry-long-tail на 17
prodcheck/scenarios/soak.sh 60                  # хаос-прогон на столько минут, в матрицу не входит
prodcheck/scenarios/loop-held.sh 20             # accounts-same-type-held до первого расхождения, не больше 20 раз
```

С `PG_VERSION=17` результаты ложатся в `build/results-pg17`, матрицу из `build/results` прогон не трогает; `results.py` выносит их во вторую секцию `RESULTS.md`.

`soak` держит `shop` и `accounts` на одном Postgres, два outbox при умеренной нагрузке, и повторяет цикл сбоев: `kill -9` одного и всех, кроме одного, выкатка, рестарт и пауза Postgres, обрыв соединений, отказ цели, записи, которые не доставятся, блокировка строки, блокировка счетов с `requeue`, которая снимается. Каждую минуту `build/results/soak.series` пишет размер базы и обоих outbox, строки и размер retry и parked, dead letters, `n_dead_tup`, число партиций, отправленное и эффекты, старое поколение каждой JVM. Проверка: ноль потерь и двойных эффектов, строгий порядок по контракту (случайные блокировки нагрузки выводят часть ключей за предел из README, больше запаркованных строк, чем `batchSize`: там порция может обогнать: ledger сверяет эффекты с замером `soak.parked`, не больше двух `batchSize` обогнавших (столько вмещает вызов), и пишет их в `past_limit_order`; вне предела порядок строгий), очередь пуста, retry и parked вернулись к порядку размера первой трети прогона (`soak_report.py`).

Сборка варианта `accounts` с метриками и админ-API: `./gradlew -p prodcheck :accounts:installDist -PaccountsVariant=full`,
ложится рядом с обычной в `accounts-full`. После правок в `common` собирайте заново все установки, `courier` с `:courier:clean`:
KSP не перегенерирует граф по изменению контроллера. Другая версия Postgres: `PG_VERSION=17 prodcheck/scenarios/run-all.sh ...`.

Запуск сам занимает общий стенд (`bench/scenarios/stand-lock.sh`), а процессы и контейнеры ведёт теми же помощниками, что и
хаос-сценарии (`bench/scenarios/chaos-lib.sh`). Базы свои, Kafka не нужна.

Каждый сценарий заканчивается машинной сверкой `scenarios/ledger.py`: дожидается, пока всё разойдётся, и сравнивает то, что
писатели закоммитили (`pc_sent`, в транзакции отправки), с тем, что видели и сделали получатели (`pc_arrival`, `pc_effect`).
Сверка падает, если сообщение потеряно, дошло из откаченной транзакции, неизвестно, пришло раньше, чем доставлено предыдущее
его ключа, нарушен порядок по ключу, эффект сделан дважды, письмо в dead-таблице не из ожидаемых, в retry или parked что-то
осталось либо дубликатов больше, чем допускает контракт. Обычный запуск инстанса на чужих часах и база на чужих часах идут
через libfaketime (`docker/`).
