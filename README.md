# cortex9-ticket-sla-escalator-go

Temporizadores de SLA para tickets de soporte: cola de prioridad por vencimiento, escalamiento por niveles, registro de escritura anticipada (WAL) y recuperación tras un reinicio sin perder ni duplicar escalamientos.

## Qué hace

El servicio acepta un ticket con prioridad y una ventana de SLA, y encola un vencimiento por cada nivel configurado para esa prioridad (por ejemplo L1 al 50 %, L2 al 80 % y L3 al incumplimiento). Un reloj inyectable permite pruebas con un reloj falso. Al vencer un nivel se registra primero la *intención* en el WAL (con `fsync` en el modo durable), después se invoca el *sink* de escalamiento y, al terminar, se registra la *completitud*. El identificador de escalamiento es `ticketID:tier` y el sink es idempotente en ese identificador.

La persistencia es un log de solo apéndice en JSON Lines, con instantánea y compactación. Si el proceso se detiene a mitad de una línea, esa línea se descarta al reabrir.

Este módulo no envía correo ni llama a un bus de mensajes: el *sink* es una interfaz. La demostración y las pruebas usan un sink en memoria.

## Uso

```bash
go test ./...
go test -race ./...
go test -bench . -benchmem -count 3
go run ./cmd/sla-escalator
```

```go
e, err := sla.Open(sla.Options{Dir: dir, Clock: sla.SystemClock{}, Sink: sink})
_ = e.Schedule(sla.Ticket{ID: "INC-1", Priority: "P1", SLA: time.Hour})
_ = e.ProcessDue()
_ = e.Resolve("INC-1")
```

Política por defecto:

- P1 y P2: L1 al 50 %, L2 al 80 %, L3 al 100 %.
- P3: L1 al 80 %, L3 al 100 %.

Los niveles se pueden configurar por prioridad.

## Recuperación

Las pruebas simulan un corte en tres puntos y reabren el mismo directorio con el mismo sink (el efecto externo ya ocurrió o no):

1. Antes de la intención: no hay efecto; al recuperar se dispara una sola vez.
2. Después de la intención y antes del sink: no hay efecto todavía; al recuperar se dispara una sola vez.
3. Después del sink y antes de la completitud: el efecto ya existe; al recuperar se vuelve a llamar al sink, que no duplica, y se registra la completitud.

## Pruebas y CI

La integración continua (GitHub Actions, sin secretos) ejecuta `go vet`, una comprobación de `gofmt`, `go test -race` y una pasada corta de benchmark (`-benchtime=1x`). Esa CI todavía no se ejecutó en GitHub.

## Benchmark

Máquina de medición: 8 CPU, Go 1.27.3 linux/amd64, `Intel(R) Xeon(R) Processor`. Comando: `go test -bench . -benchmem -count 3`.

| Iteración | ns/op | B/op | allocs/op | s/op (aprox.) |
|-----------|------:|-----:|----------:|-------------:|
| 1 | 7 452 289 030 | 5 205 274 384 | 13 108 098 | 7,45 |
| 2 | 7 298 559 796 | 5 519 898 360 | 13 108 170 | 7,30 |
| 3 | 7 429 666 534 | 5 359 148 552 | 13 108 038 | 7,43 |

Cada iteración programa 100 000 tickets P2 y dispara los tres niveles (300 000 escalamientos).

La medición de 100 000 tickets (programar y disparar los tres niveles de P2) usa `SyncNone`: el WAL se vacía en cierre y compactación, no con `fsync` en cada registro. Un corrido durable con `fsync` por apéndice no es práctico a esa escala en esta máquina.

## Licencia

Apache-2.0. Véase `LICENSE`.

---

# English

SLA timers for support tickets: a deadline min-heap, tiered escalation, an append-only write-ahead log, and crash recovery that neither loses nor duplicates escalations.

A ticket with a priority and an SLA window is scheduled into one deadline per configured tier (for example L1 at 50 %, L2 at 80 %, and L3 at breach). A clock interface supports a fake clock in tests. When a tier is due, the *intent* is written to the WAL first (fsync in durable mode), the escalation *sink* is invoked, and *completion* is recorded afterwards. The escalation id is `ticketID:tier` and the sink is idempotent on that id.

Persistence is JSON Lines plus snapshot compaction. A truncated last WAL line left by a crash is ignored on reopen.

This module does not send mail or call a message bus; the sink is an interface. The demo and the tests use an in-memory sink.

## Usage

```bash
go test ./...
go test -race ./...
go test -bench . -benchmem -count 3
go run ./cmd/sla-escalator
```

```go
e, err := sla.Open(sla.Options{Dir: dir, Clock: sla.SystemClock{}, Sink: sink})
_ = e.Schedule(sla.Ticket{ID: "INC-1", Priority: "P1", SLA: time.Hour})
_ = e.ProcessDue()
_ = e.Resolve("INC-1")
```

Default policy:

- P1 and P2: L1 at 50 %, L2 at 80 %, L3 at 100 %.
- P3: L1 at 80 %, L3 at 100 %.

Tiers are configurable per ticket priority.

## Recovery

Tests inject a crash at three points and reopen the same directory with the same sink (modeling an external idempotent side effect):

1. Before the intent: no effect; recovery fires exactly once.
2. After the intent and before the sink: no effect yet; recovery fires exactly once.
3. After the sink and before completion: the effect already exists; recovery retries the sink, which does not duplicate, and then records completion.

## Tests and CI

GitHub Actions (no secrets) runs `go vet`, a `gofmt` check, `go test -race`, and a bench smoke (`-benchtime=1x`). That workflow has never been run on GitHub.

## Benchmark

Measurement machine: 8 CPUs, Go 1.27.3 linux/amd64, `Intel(R) Xeon(R) Processor`. Command: `go test -bench . -benchmem -count 3`.

| Iteration | ns/op | B/op | allocs/op | s/op (approx.) |
|-----------|------:|-----:|----------:|--------------:|
| 1 | 7,452,289,030 | 5,205,274,384 | 13,108,098 | 7.45 |
| 2 | 7,298,559,796 | 5,519,898,360 | 13,108,170 | 7.30 |
| 3 | 7,429,666,534 | 5,359,148,552 | 13,108,038 | 7.43 |

Each iteration schedules 100,000 P2 tickets and fires all three tiers (300,000 escalations).

The 100,000-ticket run (schedule plus fire of the three P2 tiers) uses `SyncNone`: the WAL is flushed on close and compaction, not fsynced on every append. A durable fsync-per-record run of that size is not practical on this machine.

## License

Apache-2.0. See `LICENSE`.
