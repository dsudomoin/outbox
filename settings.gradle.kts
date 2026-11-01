rootProject.name = "outbox-root"

include("outbox-core", "outbox-kafka", "outbox-micrometer")
include("outbox-kora", "outbox-kafka-kora", "outbox-admin-kora") // Kora 1.x adapters
include("outbox-spring-boot", "outbox-kafka-spring-boot") // Spring Boot 3.5/4.1 adapters
include("bench") // Kotlin benchmark app, not published
