import time

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from lab.config import KAFKA_BOOTSTRAP_SERVERS


def admin():
    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def producer(**conf):
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, **conf})


def consumer(group_id, **conf):
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        **conf
    })


def describe_topic(name):
    topic = admin().list_topics(name, timeout=10).topics[name]
    return [{
        "partition": p.id,
        "leader": p.leader,
        "replicas": p.replicas,
        "isr": p.isrs,
    } for p in sorted(topic.partitions.values(), key=lambda p: p.id)]


# Crear o borrar un tema es asíncrono: el controlador lo registra y luego cada broker se entera y elige líderes.
# Hasta que todos los brokers terminan, una petición puede llegar a uno con metadatos viejos (NOT_LEADER, UNKNOWN_TOPIC).
def wait_for_topics(names, present=True, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        topics = admin().list_topics(timeout=10).topics
        if not present and not any(n in topics for n in names):
            return
        if present and all(n in topics and topics[n].error is None and topics[n].partitions and all(
                p.leader >= 0 and len(p.isrs) == len(p.replicas) for p in topics[n].partitions.values())
                           for n in names):
            return
        time.sleep(0.5)
    raise TimeoutError(f"los temas {names} no quedaron {'listos' if present else 'borrados'} en {timeout} s")


def recreate_topics(new_topics):
    """Borra los temas (si existen) y los vuelve a crear vacíos; espera a que tengan líder y réplicas al día."""
    kafka = admin()
    names = [t.topic for t in new_topics]
    existing = [n for n in names if n in kafka.list_topics(timeout=10).topics]
    if existing:
        for future in kafka.delete_topics(existing).values():
            future.result()
        wait_for_topics(existing, present=False)
    for future in kafka.create_topics(new_topics).values():
        future.result()
    wait_for_topics(names)


def watermarks(topic, partitions, retries=20):
    """(primer offset, siguiente a escribir) de cada partición. Reintenta los errores transitorios, como un cambio
    de líder recién ocurrido."""
    reader = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "group.id": "watermarks"})
    try:
        result = {}
        for partition in partitions:
            for attempt in range(retries):
                try:
                    result[partition] = reader.get_watermark_offsets(TopicPartition(topic, partition),
                                                                     timeout=10)
                    break
                except KafkaException as error:
                    if attempt == retries - 1 or not error.args[0].retriable():
                        raise
                    time.sleep(0.5)
        return result
    finally:
        reader.close()
