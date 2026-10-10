# Lección 06 - Kafka: temas, particiones, réplicas, productores, grupos de consumidores, offsets y replay.
#
# Kafka es un log distribuido: cada tema se parte en particiones, cada partición es una secuencia ordenada e inmutable
# de mensajes (con un offset cada uno) y se replica en varios brokers. Los productores escriben al final; los
# consumidores leen a su ritmo y recuerdan hasta dónde llegaron (offset confirmado). Nadie borra al leer: los
# mensajes viven hasta que vence la retención, así que varios sistemas pueden leer lo mismo y releerlo.
import json
import threading
import time
from collections import Counter, defaultdict
from datetime import date

from confluent_kafka import ConsumerGroupTopicPartitions, KafkaException, TopicPartition
from confluent_kafka.admin import ConfigResource, NewTopic

from lab import dataset
from lab.config import ALERTS_TOPIC, READINGS_TOPIC
from lab.kafka import admin, consumer, describe_topic, producer, recreate_topics, watermarks
from lab.show import step, table, title
from lab.simulator import slot_events

DAY = date(2026, 9, 30)
kafka = admin()

title("06 - Kafka")

step("El clúster: 3 brokers en modo KRaft; uno de ellos es el controlador activo (lleva los metadatos)")
cluster = kafka.describe_cluster().result()
table([{
    "broker": n.id,
    "host": f"{n.host}:{n.port}",
    "controller": n.id == cluster.controller.id
} for n in sorted(cluster.nodes, key=lambda n: n.id)])
print("cluster_id:", cluster.cluster_id)

step(f"Crear los temas: {READINGS_TOPIC} (6 particiones) y {ALERTS_TOPIC} (3), cada partición con 3 réplicas")
recreate_topics([
    NewTopic(READINGS_TOPIC,
             num_partitions=6,
             replication_factor=3,
             config={"retention.ms": str(7 * 86_400_000)}),
    NewTopic(ALERTS_TOPIC, num_partitions=3, replication_factor=3),
])
table(describe_topic(READINGS_TOPIC))
print("leader: el broker que atiende lecturas y escrituras de la partición. replicas: dónde hay copia.")
print("isr (in-sync replicas): las copias al día. Los líderes quedan repartidos entre los 3 brokers.")

step("Configuración del tema: retención, política de limpieza y réplicas mínimas sincronizadas")
resource = ConfigResource(ConfigResource.Type.TOPIC, READINGS_TOPIC)
configs = kafka.describe_configs([resource])[resource].result()
table([{
    "config": k,
    "value": configs[k].value
} for k in ("retention.ms", "cleanup.policy", "min.insync.replicas", "segment.bytes", "compression.type")])

step("Productor: un día de lecturas (96.000 mensajes) con clave = meter_id, acks=all e idempotencia")
writer = producer(
    **{
        "acks": "all",  # el líder confirma cuando todas las réplicas del ISR tienen el mensaje
        "enable.idempotence": True,  # sin duplicados aunque el productor reintente
        "linger.ms": 20,  # espera hasta 20 ms para juntar mensajes en un lote
        "compression.type": "lz4",
    })
delivered = Counter()
first_offsets = {}


def on_delivery(error, message):
    if error:
        raise KafkaException(error)
    delivered[message.partition()] += 1
    first_offsets.setdefault(message.key().decode(), (message.partition(), message.offset()))


readings = dataset.day_readings(DAY)
started = time.perf_counter()
for slot in range(dataset.SLOTS):
    for event in slot_events(DAY, slot, readings):
        writer.produce(READINGS_TOPIC,
                       key=event["meter_id"],
                       value=json.dumps(event),
                       timestamp=dataset.epoch_ms(event["ts"]),
                       on_delivery=on_delivery)
        writer.poll(0)
writer.flush()
seconds = time.perf_counter() - started
print(
    f"{sum(delivered.values()):,} mensajes en {seconds:.1f} s ({sum(delivered.values()) / seconds:,.0f} msg/s)"
)
table([{"partition": p, "messages": n} for p, n in sorted(delivered.items())])

step("La clave decide la partición (hash(clave) % particiones): un medidor siempre cae en la misma")
table([{
    "meter_id": m,
    "partition": first_offsets[m][0],
    "first_offset": first_offsets[m][1]
} for m in ("M00001", "M00002", "M00003", "M00500", "M01000")])
print("Kafka solo garantiza orden dentro de una partición: las 96 lecturas de cada medidor llegan en orden.")

step("Offsets por partición: el primero y el siguiente a escribir (high watermark)")
partitions = [TopicPartition(READINGS_TOPIC, p) for p in range(6)]
offsets = watermarks(READINGS_TOPIC, range(6))
latest = {p: high for p, (low, high) in offsets.items()}
table([{"partition": p, "earliest": low, "latest": high} for p, (low, high) in sorted(offsets.items())])

step("Grupo de consumidores: dos consumidores del grupo 'dashboard' se reparten las 6 particiones")
stats = defaultdict(lambda: {"messages": 0, "partitions": set(), "assigned": []})


def consume(name, group, limit=None, idle_seconds=4):
    reader = consumer(group, **{"enable.auto.commit": False})
    reader.subscribe([READINGS_TOPIC],
                     on_assign=lambda c, ps: stats[name]["assigned"].append(sorted(p.partition for p in ps)))
    last = time.monotonic()
    while time.monotonic() - last < idle_seconds and (limit is None or stats[name]["messages"] < limit):
        batch = reader.consume(num_messages=500, timeout=0.5)
        for message in batch:
            stats[name]["messages"] += 1
            stats[name]["partitions"].add(message.partition())
        if batch:
            reader.commit(asynchronous=False)  # confirmar: "ya procesé hasta aquí"
            last = time.monotonic()
    reader.close()


threads = [threading.Thread(target=consume, args=(f"dashboard-{i}", "dashboard")) for i in (1, 2)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
table([{
    "consumer": name,
    "messages": s["messages"],
    "final_assignment": s["assigned"][-1] if s["assigned"] else []
} for name, s in sorted(stats.items())])
print(
    "Cada partición la lee un solo consumidor del grupo. Si uno se cae, el grupo se rebalancea y otro la toma."
)
print("Más consumidores que particiones no sirve de nada: los que sobran quedan sin asignación.")

step("Otro grupo ('billing') lee el mismo tema de forma independiente; procesa 30.000 mensajes y se detiene")
consume("billing", "billing", limit=30_000)
print("leídos:", stats["billing"]["messages"])

step("Lag: mensajes que a cada grupo le faltan por leer (high watermark - offset confirmado)")
rows = []
for group in ("dashboard", "billing"):
    request = ConsumerGroupTopicPartitions(group, partitions)
    committed = kafka.list_consumer_group_offsets([request])[group].result().topic_partitions
    for tp in committed:
        committed_offset = tp.offset if tp.offset >= 0 else None  # -1001: el grupo nunca confirmó en esa partición
        rows.append({
            "group": group,
            "partition": tp.partition,
            "committed": "-" if committed_offset is None else committed_offset,
            "latest": latest[tp.partition],
            "lag": latest[tp.partition] - (committed_offset or 0)
        })
table(rows)
print(
    "billing se detuvo a mitad: el consumidor recibe lotes de pocas particiones a la vez, así que unas quedaron"
)
print(
    "completas y otras sin empezar. Al volver a arrancar, el grupo sigue exactamente desde su offset confirmado."
)

step(
    "Replay: buscar el offset de un instante (offsets_for_times) y releer desde ahí, sin afectar a los grupos"
)
reader = consumer("replay", **{"enable.auto.commit": False})
evening = dataset.epoch_ms(f"{DAY} 18:00:00")
positions = reader.offsets_for_times([TopicPartition(READINGS_TOPIC, p, evening) for p in range(6)],
                                     timeout=10)
reader.assign(positions)
messages = reader.consume(num_messages=5, timeout=10)
for message in messages:
    print(f"partition={message.partition()} offset={message.offset()} key={message.key().decode()} "
          f"value={message.value().decode()}")
reader.close()
print(
    "El timestamp de cada mensaje es la hora de la lectura (el productor lo fijó), así que '18:00' es la hora del"
)
print(
    "evento. Así se reprocesa un periodo después de corregir un error, sin volver a pedir los datos al origen."
)

step("Los grupos de consumidores que conoce el clúster")
groups = kafka.describe_consumer_groups(["dashboard", "billing"])
table([{
    "group": g,
    "state": str(f.result().state).split(".")[-1],
    "members": len(f.result().members)
} for g, f in groups.items()])
print("\nSiguiente: python lessons/07_spark_streaming.py")
