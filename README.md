# Kafka Pilot

A visual guide to configuring Apache Kafka®. Answer a few plain questions, watch a live diagram of producers, brokers and consumers react, and copy working config files for your language.

It is a single HTML page with no build step and no server. Nothing you type leaves the page.

**Live version:** https://claude.ai/artifact/X8zLdwWsTi7grAXyaEg11m (private until shared from the page's Share menu)

## What it does

### Five steps

| Step | You decide | Kafka settings it writes |
|---|---|---|
| Goal | Kind of data, delivery guarantee, topic name | applies a full set of defaults |
| Cluster | Brokers, partitions, copies, retention, compaction, zones, security | `num.partitions`, `replication.factor`, `min.insync.replicas`, `retention.ms/bytes`, `cleanup.policy`, `broker.rack`, `security.protocol`, … |
| Producer | Apps sending, acks, batching, compression, keys, retries, transactions | `acks`, `enable.idempotence`, `linger.ms`, `batch.size`, `compression.type`, `retries`, `delivery.timeout.ms`, `transactional.id`, `buffer.memory`, … |
| Consumer | Groups, consumers, speed, partition sharing, commits, failures | `group.id`, `auto.offset.reset`, `enable.auto.commit`, `max.poll.records`, `session.timeout.ms`, `partition.assignment.strategy`, `group.instance.id`, `isolation.level`, … |
| Your files | Language | ready-to-copy files |

**Kinds of data** (each a full preset): Getting started, Never lose a message, Payment system, Balanced, Move lots of data, Lowest delay, Background jobs, Sensor and device data, Database changes (CDC), Audit and history log.

**Delivery guarantees:** at most once, at least once and exactly once. Choosing one applies the standard settings; a side-by-side comparison shows each guarantee's promise, what a crash does, the settings and the standard code in Java or Python.

### The live diagram

- **Your apps · write:** producers, the `hash % N` partitioner and an acks panel.
- **Kafka cluster · store:** broker cards you can switch off, and every partition drawn as an append-only log with offsets. Dashed slots are not yet copied to every in-sync replica; crossed-out slots were removed by compaction.
- **Your services · read:** consumer groups, each with its own lane, read position and settings.

Messages, confirmations, refused writes, retries, replication and rebalances are animated. A lag panel shows each group's lag and trend and suggests a fix you can apply with one click.

### Also

- **Toolbar** above the diagram for producers, brokers, partitions, copies, consumers and groups.
- **Five challenges:** survive a broker failure, stop growing lag, keep orders in order, replay history, scale with fewer pauses.
- **Import:** paste an existing config (`.properties`, Spring `application.yml` or `KAFKA_` variables) on any step to load and check it.
- **Share:** a link or short code that restores the same setup.
- **Tooltips** on every part of the diagram and every control.
- **Light, dark or automatic theme.** Dark Reader is detected and asked to step aside, so the page uses its own dark colours.

### Generated files

`producer.properties`, `consumer.properties`, `ConsumerLoop.java`, Spring Boot `application.yml`, Python (`confluent-kafka`), Node.js (`@confluentinc/kafka-javascript`), Go (`confluent-kafka-go`), `server.properties` (KRaft), `docker-compose.yml` and setup commands. Every setting carries a comment saying what it does.

## Use it

Open `index.html` through the live link above, or build a standalone copy (see [Build](#build)) and open `dist/kafka-pilot.html` in a browser. The page loads GSAP from cdnjs and fonts from Google Fonts; it works without them, with less animation and a fallback font.

## Project layout

```
index.html                     the whole app: markup, styles and script
tests/run.js                   browser tests (Playwright)
tests/kafka-smoke.sh           checks the generated files against a real Kafka cluster in Docker
tests/kafka/dump.js            writes the files the page generates for one setup
tests/shot.js                  screenshot helper for visual review
scripts/build.js               builds dist/ (standalone page + zip)
scripts/deploy-artifactory.sh  uploads dist/ to a JFrog Artifactory generic repository
DESIGN.md                      design reference the visual style follows
THIRD_PARTY_LICENSE-*.txt      licences for the icons and the Kafka logo
```

## Tests

Install the one dev dependency, then run the suites with Node directly:

```bash
npm install
node tests/run.js            # browser tests, about 3 minutes
bash tests/kafka-smoke.sh    # needs Docker; about 10 minutes
```

`node tests/run.js <name>` runs only the tests whose name contains `<name>`.

**Browser tests** (`tests/run.js`) load the page in headless Chromium and fail on any error. They cover: every step at four screen sizes with no sideways scrolling, every kind of data answering every question, the partitioner against Kafka's own murmur2 test vectors, a tooltip on every control, lag growing under load and draining after the suggested fixes, consumer groups, broker failure, Kafka's partition assignors, share links, all five challenges, compaction and security in the files, the delivery guarantees, pasted configs, and that every generated JavaScript and Python file is valid code.

**Kafka smoke test** (`tests/kafka-smoke.sh`) starts the generated `docker-compose.yml` (three KRaft brokers, `apache/kafka:3.9.0`) and checks, among other things, that:

- the topic is created as the page describes, brokers run in zones and the size limit is set;
- the Java, Python and Node clients work with the generated settings, and every key lands in the partition the page predicts;
- a compacted topic refuses messages without a key;
- the Java and librdkafka clients accept the generated TLS and SASL/SCRAM settings;
- a transaction commits and an aborted one stays invisible, and the generated exactly-once loop turns every input into exactly one committed result;
- writes continue with one broker down, and Kafka answers `NOT_ENOUGH_REPLICAS` when too few copies remain.

Not covered by the smoke test: the Go and Spring files are not run, and the security settings are checked for being accepted, not against a secured broker.

## Build

```bash
node scripts/build.js
```

Writes `dist/kafka-pilot.html` (the page as a standalone document) and `dist/kafka-pilot-<version>.zip` with the licence notices.

To upload both to a JFrog Artifactory generic repository under `kafka-pilot/<version>/`:

```bash
ARTIFACTORY_URL=https://yourcompany.jfrog.io/artifactory \
ARTIFACTORY_REPO=generic-local \
ARTIFACTORY_TOKEN_FILE=~/.artifactory-token \
bash scripts/deploy-artifactory.sh
```

## Limits

- The animation is a simplified simulation. It shows cause and effect; its rates and offsets are not real throughput.
- The generated cluster uses Kafka 3.9, so the new consumer rebalance protocol (`group.protocol=consumer`, Kafka 4.0) is not offered.
- Security files use placeholders (`/etc/kafka/certs/ca.pem`, password `change-me`) that you replace before deploying.

## Licences and trademarks

Icons are from [Tabler Icons](https://tabler.io/icons) (MIT). The Kafka logo is the version published on kafka.apache.org (Apache License 2.0) and is used only as a link to the project, following the [ASF trademark policy](https://www.apache.org/foundation/marks/). See the `THIRD_PARTY_LICENSE-*.txt` files.

Apache®, Apache Kafka, Kafka®, and the Kafka logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by the Apache Software Foundation is implied by the use of these marks. Kafka Pilot is an independent learning tool.
