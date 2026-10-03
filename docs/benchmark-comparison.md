# Doctrine and broker comparison

The `paired-v3-immediate` comparison is **inconclusive**. The broker completed all repetitions, but Doctrine failed two measured concurrent repetitions. These results do not establish an overall performance advantage.

## Configuration

Both backends use real Symfony Messenger transports, matching payloads and handler work, WAL/FULL durability, immediate transactions, and a 5000ms SQLite busy timeout. Doctrine uses PHP 8.5's native PDO transaction-mode option, not a custom transport. The reference application's empty-poll optimization is not included.

The run used PHP 8.5.10, SQLite 3.45.1, an Intel i5-13400F, and ext4 storage on NVMe. Xdebug was disabled through `XDEBUG_MODE=off`. Effective modes were checked for benchmark roles. The SQLite child's mode was verified indirectly through the same Amp context factory and interpreter, not a diagnostic call to that worker.

All six workloads ran with one warmup and three measured repetitions per backend. Pair order alternated. The [method](benchmark-method.md) defines the workloads, accounting, and acceptance criteria. Single-queue consumers use notification waits; multi-queue backlog and delayed workloads use polling. The delayed workload includes a separate diagnostic queue and does not measure notification-driven delayed pickup.

## Results

ACKs per second below are ranges across completed measured repetitions. They describe finite batch windows, not sustained capacity. The smaller workloads run too briefly to support a steady-state throughput claim.

| Workload | Doctrine ACK/s | Broker ACK/s |
| --- | ---: | ---: |
| Roundtrip | 112–117 | 105–108 |
| Concurrent, 3000 messages | 119, one completed repetition | 138–140 |
| Four publishers to one consumer | 155–248 | 177–223 |
| Backlog drain | 188–242 | 199–233 |

Doctrine's other two concurrent repetitions recorded `database is locked` consumer failures. Their samples and process failures remain in the archive, even where the remaining consumer completed delivery. They are not successful performance repetitions.

Broker concurrent handling windows were 21.4–21.7 seconds. The clean Doctrine window was 25.1 seconds. One clean Doctrine repetition is insufficient to establish a repeatable advantage. Other throughput ranges overlap, and roundtrip throughput is lower for the broker.

The broker's sampled process-tree RSS was roughly 90–100 MiB higher. Notification waits reduced empty polling, but the measured idle pickup latency did not establish a consistent improvement. Idle and delayed publication rates are paced and are not capacity measurements. Small workload sample counts do not support strong p99 claims.

## Reproduce and inspect

Run the fixed matrix from the repository root:

```sh
	XDEBUG_MODE=off vendor/bin/castor bench
```

The command returns nonzero if any repetition fails. Do not retry until all results are green or discard failed repetitions.

The complete capture is committed as [paired-v3-20261003.tar.gz](../bench/results/paired-v3-20261003.tar.gz). It contains `20261003-040336-da069e7f/summary.json`, `report.md`, per-repetition records, and process logs. The measured checkout was the Task 08 implementation based on `8a15e9d`, with uncommitted changes recorded in its metadata.

Archive SHA-256:

```text
7057bc1ed886fa1b47310feccf82225dafadba9541a6b0a9d0742b74e49bc36a
```

Earlier DEFERRED and Xdebug-develop captures are diagnostic runs with different configurations. They are not pooled with this comparison. The archive above includes every scheduled repetition of the final fixed matrix, including failures.
