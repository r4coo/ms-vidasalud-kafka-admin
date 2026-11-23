# ms-vidasalud-kafka-admin

Microservicio de **administración de Kafka** de VidaSalud (puerto 8089). Equivale, para Kafka, a lo que
`ms-vidasalud-rabbit-admin` hace para RabbitMQ. Usa el `AdminClient` oficial contra el clúster de 3 brokers.

Solo acepta llamadas del rol **Admin** (el BFF valida el JWT de Entra ID y propaga `X-User-Roles`; este servicio
vuelve a comprobarlo).

| Método | Ruta (`/api/admin/kafka`) | Descripción |
|---|---|---|
| GET | `/cluster` | Id del clúster, controlador y brokers |
| GET | `/topics` | Lista de tópicos (particiones y factor de replicación) |
| GET | `/topics/{nombre}` | Líder, réplicas e ISR por partición y configuración |
| POST | `/topics` | Crea un tópico: `nombre`, `particiones` (1-12), `factorReplicacion` (1-3), `configuracion` |
| PUT | `/topics/{nombre}/config` | Modifica parámetros (`retention.ms`, `cleanup.policy`, ...) |
| PUT | `/topics/{nombre}/partitions` | Aumenta el total de particiones (Kafka no permite reducirlas) |
| DELETE | `/topics/{nombre}` | Elimina un tópico (los internos `__*` están protegidos) |
| GET | `/groups` | Grupos de consumidores y número de miembros |
| GET | `/groups/{grupo}` | Miembros, offset confirmado, offset final y **lag** por partición |

Parámetros permitidos: `cleanup.policy` (`delete`, `compact`, `compact,delete`), `retention.ms`, `retention.bytes`,
`segment.ms`, `segment.bytes`, `min.insync.replicas`, `min.cleanable.dirty.ratio`, `delete.retention.ms`,
`max.message.bytes`. Cualquier otro se rechaza con 400.

Errores: 400 (validación), 403 (sin rol Admin), 404 (tópico/grupo inexistente), 409 (tópico existente o protegido),
503 (el clúster no responde).

Variables: `KAFKA_BOOTSTRAP_SERVERS` (por defecto `localhost:9092`), `SERVER_PORT`, `KAFKA_ADMIN_TIMEOUT_MS`.

Pruebas: `./gradlew test`. Imagen: `docker build -t ms-vidasalud-kafka-admin .`
