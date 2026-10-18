#!/bin/bash
# Runs the files Kafka Pilot generates against a real Kafka cluster in Docker.
# Checks: the compose file starts, the setup command creates the topic, producer and consumer
# properties work with the Kafka CLI, keys land where the page says, the Python client config is
# accepted by librdkafka, and a write is refused when fewer than min.insync.replicas copies are up.
set -uo pipefail
cd "$(dirname "$0")/.."
OUT=tests/out/kafka; P=kpsmoke; FAIL=0
pass() { echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; FAIL=1; }
rm -rf "$OUT"; node tests/kafka/dump.js "$OUT" balanced range >/dev/null
node tests/kafka/dump.js "$OUT/sticky" balanced sticky >/dev/null
DC="docker compose -p $P -f $OUT/docker-compose.yml"
cleanup() { $DC down -v --remove-orphans >/dev/null 2>&1; }
trap cleanup EXIT
cleanup
K() { $DC exec -T "$1" /opt/kafka/bin/"${@:2}"; }

echo "docker-compose.yml"
$DC up -d >/dev/null 2>&1 && pass "3 brokers started" || { fail "compose up failed"; exit 1; }
for i in $(seq 60); do K kafka1 kafka-topics.sh --bootstrap-server kafka1:9092 --list >/dev/null 2>&1 && break; sleep 2; done
K kafka1 kafka-broker-api-versions.sh --bootstrap-server kafka1:9092 2>/dev/null | grep -c "^kafka" | grep -q 3 && pass "all 3 brokers answer" || fail "not all brokers answer"

echo "setup commands"
CREATE=$(grep '^# docker compose exec' "$OUT/docker-compose.yml" | sed 's/^# docker compose exec kafka1 \/opt\/kafka\/bin\///')
eval "K kafka1 $CREATE" >/dev/null 2>&1 && pass "topic created with the generated command" || fail "topic create failed: $CREATE"
DESC=$(K kafka1 kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic orders 2>/dev/null)
echo "$DESC" | grep -q "PartitionCount: 6" && echo "$DESC" | grep -q "ReplicationFactor: 3" && echo "$DESC" | grep -q "min.insync.replicas=2" \
  && pass "6 partitions, 3 copies, min.insync.replicas=2, as the page says" || fail "topic layout differs: $DESC"

echo "producer.properties and consumer.properties"
for f in producer.properties consumer.properties; do $DC cp "$OUT/$f" "kafka1:/tmp/$f" >/dev/null; done
sed 's/^group.id=.*/group.id=sticky-check/' "$OUT/sticky/consumer.properties" > "$OUT/consumer-sticky.properties"
$DC cp "$OUT/consumer-sticky.properties" kafka1:/tmp/consumer-sticky.properties >/dev/null
KEYS=$(node -e "console.log(Object.keys(require('./$OUT/expected.json')).map(k => k + ':hello-' + k).join('\n'))")
PRODLOG=$(echo "$KEYS" | K kafka1 kafka-console-producer.sh --bootstrap-server kafka1:9092 --topic orders --producer.config /tmp/producer.properties \
  --property parse.key=true --property key.separator=: 2>&1)
echo "$PRODLOG" | grep -qiE "error|exception" && fail "producer reported: $PRODLOG" || pass "producer.properties accepted, 5 keyed messages sent"
GOT=$(K kafka1 kafka-console-consumer.sh --bootstrap-server kafka1:9092 --topic orders --consumer.config /tmp/consumer.properties \
  --from-beginning --max-messages 5 --timeout-ms 30000 --property print.key=true --property print.partition=true 2>/dev/null)
[ "$(echo "$GOT" | grep -c hello)" = 5 ] && pass "consumer.properties read all 5 back" || fail "consumer read: $GOT"
node -e "
const exp = require('./$OUT/expected.json'), bad = [];
for (const line of process.argv[1].split('\n')) { const m = line.match(/Partition:(\d+)\s+(\S+)/); if (m && exp[m[2]] !== +m[1]) bad.push(m[2] + ' page P' + exp[m[2]] + ' kafka P' + m[1]); }
if (bad.length) { console.log('  ✗ keys landed elsewhere: ' + bad.join(', ')); process.exit(1); } console.log('  ✓ every key landed in the partition the page predicts');" "$GOT" || FAIL=1
GOT2=$(K kafka1 kafka-console-consumer.sh --bootstrap-server kafka1:9092 --topic orders --consumer.config /tmp/consumer-sticky.properties \
  --from-beginning --max-messages 5 --timeout-ms 30000 2>&1)
[ "$(echo "$GOT2" | grep -c hello)" = 5 ] && pass "cooperative-sticky consumer.properties works too" || fail "sticky consumer: $GOT2"

echo "kafka_client.py (confluent-kafka / librdkafka)"
NET=$(docker network ls --format '{{.Name}}' | grep "^${P}_" | head -1)
# run the generated script up to its endless loop, then read once and stop
sed '/^while True:/,$d' "$OUT/kafka_client.py" > "$OUT/py_check.py"
cat >> "$OUT/py_check.py" <<'PY'
got = []
for _ in range(20):
    for m in consumer.consume(num_messages=10, timeout=1.0):
        if not m.error(): got.append(m.key())
    if len(got) >= 5: break
consumer.close()
print("PYREAD", len(got))
PY
PYOUT=$(docker run --rm --network "$NET" -v "$PWD/$OUT:/w" python:3.12-slim sh -c "pip install -q confluent-kafka >/dev/null 2>&1 && python /w/py_check.py" 2>&1)
echo "$PYOUT" | grep -q "PYREAD [5-9]" && pass "Python producer and consumer configs accepted; read $(echo "$PYOUT" | grep -o 'PYREAD [0-9]*' | cut -d' ' -f2) messages" || fail "python: $(echo "$PYOUT" | tail -5)"

echo "kafka-client.js (@confluentinc/kafka-javascript)"
# the generated file, with handle() defined to count messages and stop after the 5 test keys
{ echo 'let seen = 0; async function handle(m) { if (++seen >= 5) { console.log("NODEREAD", seen); process.exit(0); } }'; \
  echo 'setTimeout(() => { console.log("NODEREAD", seen); process.exit(1); }, 60000);'; \
  sed "s/'group.id': '[^']*'/'group.id': 'node-check'/" "$OUT/kafka-client.js"; } > "$OUT/node_check.js"
NODEOUT=$(docker run --rm --network "$NET" -v "$PWD/$OUT:/w" -w /tmp node:22-slim sh -c "npm i -s @confluentinc/kafka-javascript >/dev/null 2>&1 && cp /w/node_check.js . && node node_check.js" 2>&1)
echo "$NODEOUT" | grep -q "NODEREAD [5-9]" && pass "Node producer and consumer configs accepted; read 5 messages" || fail "node: $(echo "$NODEOUT" | tail -5)"

echo "a broker goes down"
K kafka1 kafka-topics.sh --bootstrap-server kafka1:9092 --create --topic strict --partitions 1 --replication-factor 3 --config min.insync.replicas=3 >/dev/null 2>&1
$DC stop kafka3 >/dev/null 2>&1; sleep 8
OK=$(echo "k:still-works" | K kafka1 kafka-console-producer.sh --bootstrap-server kafka1:9092 --topic orders --producer.config /tmp/producer.properties \
  --property parse.key=true --property key.separator=: --producer-property delivery.timeout.ms=20000 --producer-property request.timeout.ms=5000 2>&1)
echo "$OK" | grep -qiE "error|exception" && fail "with 1 of 3 brokers off, orders should still accept writes: $OK" || pass "1 broker off: writes still accepted (2 copies >= min.insync.replicas=2)"
REF=$(echo "k:refused" | K kafka1 kafka-console-producer.sh --bootstrap-server kafka1:9092 --topic strict --producer.config /tmp/producer.properties \
  --property parse.key=true --property key.separator=: --producer-property delivery.timeout.ms=15000 --producer-property request.timeout.ms=5000 2>&1)
echo "$REF" | grep -qiE "NOT_ENOUGH_REPLICAS|NotEnoughReplicas" && pass "min.insync.replicas=3 with a broker off: Kafka answers NOT_ENOUGH_REPLICAS, as the diagram shows" || fail "expected NOT_ENOUGH_REPLICAS, got: $(echo "$REF" | tail -3)"

[ $FAIL = 0 ] && echo "all passed" || echo "FAILED"
exit $FAIL
