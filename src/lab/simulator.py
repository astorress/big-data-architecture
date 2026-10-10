# Simulador del sistema de medición: reproduce un día de lecturas en Kafka como si llegaran en tiempo real.
#
# Cada "intervalo" de 15 minutos del día se envía de golpe (los 1.000 medidores reportan a la vez) y luego se espera
# `seconds_per_slot` segundos reales. Con 1 s por intervalo, un día completo dura 96 s. El tiempo del evento (ts) es el
# de la lectura, no el de envío: es lo que usan las ventanas de Spark Structured Streaming.
import json
import threading

from lab import dataset
from lab.config import READINGS_TOPIC
from lab.kafka import producer

LATE_FEEDER = "BOG-01"
# Los medidores pares de BOG-01 cuelgan de un concentrador con problemas: sus lecturas llegan tarde
LATE_METERS = {
    m["meter_id"]
    for m in dataset.METER_LIST if m["feeder"] == LATE_FEEDER and int(m["meter_id"][1:]) % 2 == 0
}


def slot_events(day, slot, readings=None):
    """Las lecturas de los 1.000 medidores en un intervalo de 15 minutos, como diccionarios listos para JSON."""
    kwh, voltage = readings or dataset.day_readings(day)
    ts = dataset.slot_time(day, slot).strftime("%Y-%m-%d %H:%M:%S")
    return [{
        "meter_id": m["meter_id"],
        "ts": ts,
        "kwh": float(kwh[i, slot]),
        "voltage": float(voltage[i, slot])
    } for i, m in enumerate(dataset.METER_LIST)]


class Replay(threading.Thread):
    """Envía las lecturas de `day` a Kafka en segundo plano, intervalo por intervalo.

    late_slots: cuántos intervalos tarde llegan las lecturas de LATE_METERS (0 = todas a tiempo).
    inject:     función (slot, eventos) -> eventos para meter incidentes en vivo (lección 10).
    """

    def __init__(self,
                 day,
                 start_slot=0,
                 slots=dataset.SLOTS,
                 seconds_per_slot=1.0,
                 late_slots=0,
                 inject=None):
        super().__init__(daemon=True)
        self.day, self.start_slot, self.slots = day, start_slot, slots
        self.seconds_per_slot, self.late_slots, self.inject = seconds_per_slot, late_slots, inject
        self.sent, self.current_slot = 0, None
        self._stop_event = threading.Event()
        self._readings = dataset.day_readings(day)

    def run(self):
        kafka = producer(**{"linger.ms": 20, "compression.type": "lz4", "acks": "all"})
        delayed = []
        for slot in range(self.start_slot, min(self.start_slot + self.slots, dataset.SLOTS)):
            if self._stop_event.is_set():
                break
            self.current_slot = slot
            batch = slot_events(self.day, slot, self._readings)
            if self.inject:
                batch = self.inject(slot, batch)
            if self.late_slots:
                delayed.append([e for e in batch if e["meter_id"] in LATE_METERS])
                batch = [e for e in batch if e["meter_id"] not in LATE_METERS]
                if len(delayed) > self.late_slots:
                    batch += delayed.pop(0)
            for event in batch:
                kafka.produce(READINGS_TOPIC, key=event["meter_id"], value=json.dumps(event))
                kafka.poll(0)
            kafka.flush()
            self.sent += len(batch)
            self._stop_event.wait(self.seconds_per_slot)
        kafka.flush()

    def stop(self):
        self._stop_event.set()
        self.join()
