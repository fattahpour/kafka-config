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
rm -rf "$OUT"; node tests/kafka/dump.js "$OUT" balanced range none '{"racks":true,"retBytes":1}' >/dev/null
node tests/kafka/dump.js "$OUT/pay" payments range none '{"staticMember":true,"racks":true}' >/dev/null
node tests/kafka/dump.js "$OUT/sticky" balanced sticky >/dev/null
node tests/kafka/dump.js "$OUT/cdc" cdc range >/dev/null
node tests/kafka/dump.js "$OUT/sec" balanced range sasl >/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$OUT/sec/ca.key" -out "$OUT/sec/ca.pem" -days 1 -subj "/CN=kafka-pilot-test-ca" >/dev/null 2>&1
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
echo "$DESC" | grep -q "retention.bytes=1073741824" && pass "size limit retention.bytes=1 GB set on the topic" || fail "retention.bytes missing: $DESC"
RACKS=$(for b in 1 2 3; do K kafka1 kafka-configs.sh --bootstrap-server kafka1:9092 --describe --entity-type brokers --entity-name $b --all 2>/dev/null | grep -E "^ *broker.rack=" | head -1; done)
[ "$(echo "$RACKS" | grep -c 'broker.rack=zone-')" = 3 ] && pass "brokers run in zones: $(echo $RACKS | tr '\n' ' ')" || fail "broker.rack not set: $RACKS"

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

echo "compaction (Database changes)"
CCREATE=$(grep '^# docker compose exec' "$OUT/cdc/docker-compose.yml" | sed 's/^# docker compose exec kafka1 \/opt\/kafka\/bin\///; s/--topic orders /--topic orders-compact /')
eval "K kafka1 $CCREATE" >/dev/null 2>&1 && K kafka1 kafka-configs.sh --bootstrap-server kafka1:9092 --describe --topic orders-compact 2>/dev/null | grep -q "cleanup.policy=compact" \
  && pass "generated command creates a compacted topic" || fail "compacted topic not created: $CCREATE"
NOKEY=$(echo "no-key-here" | K kafka1 kafka-console-producer.sh --bootstrap-server kafka1:9092 --topic orders-compact --producer.config /tmp/producer.properties \
  --producer-property delivery.timeout.ms=15000 --producer-property request.timeout.ms=5000 2>&1)
echo "$NOKEY" | grep -qiE "without key|CORRUPT_MESSAGE|InvalidRecord" && pass "a message without a key is refused, as the page warns" || fail "unkeyed message was not refused: $(echo "$NOKEY" | tail -2)"

echo "security settings (SASL/SCRAM over TLS)"
sed "s#^ssl.truststore.location=.*#ssl.truststore.location=/tmp/ca.pem#" "$OUT/sec/producer.properties" > "$OUT/sec/p.properties"
$DC cp "$OUT/sec/p.properties" kafka1:/tmp/p-sasl.properties >/dev/null; $DC cp "$OUT/sec/ca.pem" kafka1:/tmp/ca.pem >/dev/null
JSEC=$(printf '' | K kafka1 kafka-console-producer.sh --bootstrap-server kafka1:9092 --topic orders --producer.config /tmp/p-sasl.properties 2>&1)
echo "$JSEC" | grep -qE "ConfigException|Failed to construct|KafkaException|LoginException" && fail "Java client rejected the security settings: $(echo "$JSEC" | grep -E 'Exception' | head -2)" \
  || pass "Java client loads security.protocol, the PEM truststore and the SCRAM login"
cat > "$OUT/sec/py_sec.py" <<'PY'
import ast, re
from confluent_kafka import Producer, Consumer
src = open('/w/kafka_client.py').read()
for name, cls in (('producer', Producer), ('consumer', Consumer)):
    body = re.search(name + r' = \w+\((\{.*?\n\})\)', src, re.S).group(1)
    conf = ast.literal_eval(re.sub(r'#[^\n]*', '', body))
    conf['ssl.ca.location'] = '/w/ca.pem'
    c = cls(conf)
    print('PYSEC', name, conf['security.protocol'], conf['sasl.mechanisms'])
PY
PYSEC=$(docker run --rm -v "$PWD/$OUT/sec:/w" python:3.12-slim sh -c "pip install -q confluent-kafka >/dev/null 2>&1 && python /w/py_sec.py" 2>&1)
[ "$(echo "$PYSEC" | grep -c '^PYSEC .* sasl_ssl SCRAM-SHA-512')" = 2 ] && pass "librdkafka accepts the Python security settings" || fail "python security: $(echo "$PYSEC" | tail -3)"

echo "exactly once (Payment system: transactions, static membership, zones)"
K kafka1 kafka-topics.sh --bootstrap-server kafka1:9092 --create --topic tx-check --partitions 3 --replication-factor 3 --config min.insync.replicas=2 >/dev/null 2>&1
cat > "$OUT/pay/py_tx.py" <<'PY'
import ast, re
from confluent_kafka import Producer, Consumer
src = open('/w/kafka_client.py').read()
conf = {n: ast.literal_eval(re.sub(r'#[^\n]*', '', re.search(n + r' = \w+\((\{.*?\n\})\)', src, re.S).group(1))) for n in ('producer', 'consumer')}
assert conf['producer'].get('transactional.id') and conf['consumer'].get('isolation.level') == 'read_committed', 'transaction settings missing'
assert conf['consumer'].get('group.instance.id') and conf['consumer'].get('client.rack'), 'static membership or zone missing'
p = Producer(conf['producer']); p.init_transactions()
p.begin_transaction()
for i in range(3): p.produce('tx-check', key=f'acct-{i}', value=f'committed-{i}')
p.commit_transaction()
p.begin_transaction(); p.produce('tx-check', key='acct-x', value='aborted'); p.abort_transaction()
c = Consumer({**conf['consumer'], 'group.id': 'tx-check-group'}); c.subscribe(['tx-check'])
got = []
for _ in range(30):
    for m in c.consume(num_messages=10, timeout=1.0):
        if not m.error(): got.append(m.value().decode())
    if len(got) >= 3: break
for _ in range(3): got += [m.value().decode() for m in c.consume(num_messages=10, timeout=1.0) if not m.error()]
c.close()
print('PYTX', sorted(got))
PY
PYTX=$(docker run --rm --network "$NET" -v "$PWD/$OUT/pay:/w" python:3.12-slim sh -c "pip install -q confluent-kafka >/dev/null 2>&1 && python /w/py_tx.py" 2>&1)
echo "$PYTX" | grep -q "PYTX \['committed-0', 'committed-1', 'committed-2'\]" \
  && pass "a transaction commits 3 messages, an aborted one stays invisible to read_committed; static membership and client.rack accepted" \
  || fail "transactions: $(echo "$PYTX" | tail -3)"

echo "exactly once end to end: the generated Python loop"
RES=$(grep -A2 '# 3a. Output topic' "$OUT/pay/setup.sh" | tail -2 | tr -d '\\\n' | sed 's#bin/##; s#kafka1:9092#kafka1:9092#')
eval "K kafka1 $RES" >/dev/null 2>&1
{ echo 'def process(m): return m.value()'; sed "s/\"group.id\": \"[^\"]*\"/\"group.id\": \"eos-check\"/" "$OUT/pay/kafka_client.py"; } > "$OUT/pay/py_eos.py"
docker run --rm --network "$NET" -v "$PWD/$OUT/pay:/w" python:3.12-slim sh -c "pip install -q confluent-kafka >/dev/null 2>&1 && timeout 45 python /w/py_eos.py" >/dev/null 2>&1
CNT() { K kafka1 kafka-console-consumer.sh --bootstrap-server kafka1:9092 --topic "$1" --from-beginning --isolation-level read_committed --timeout-ms 15000 2>/dev/null | wc -l; }
IN=$(CNT orders); OUTN=$(CNT orders.results)
[ "$IN" -gt 0 ] && [ "$IN" = "$OUTN" ] && pass "every one of $IN input messages produced exactly one committed result ($OUTN)" || fail "exactly once: $IN in, $OUTN results"

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
