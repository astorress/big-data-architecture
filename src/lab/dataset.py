# Conjunto de datos sintético: la medición inteligente (AMI) de una distribuidora de energía en Colombia.
#
#   meters       1.000 medidores: tipo de cliente, estrato, ciudad, circuito (feeder) y potencia contratada
#   readings     una lectura cada 15 minutos por medidor durante septiembre de 2026 (2,88 millones de filas)
#   weather      temperatura horaria por ciudad
#   inspections  resultado de 300 visitas de campo hechas en octubre (las etiquetas para el modelo de fraude)
#
# Todo sale de una semilla fija: cada ejecución genera exactamente los mismos datos. Por eso las lecciones pueden
# comparar los resultados de un modelo con la verdad (FRAUD, STUCK), algo que en la vida real no se conoce.
import random
from datetime import date, datetime, timedelta, timezone

import numpy as np

SEED = 42
METERS = 1000
START = date(2026, 9, 1)
DAYS = 30
SLOTS = 96  # lecturas de 15 minutos por día
TZ = timezone(timedelta(hours=-5),
              "America/Bogota")  # hora local de las lecturas (Colombia no tiene horario de verano)

# ciudad: (código, peso, temperatura media, amplitud diaria)
CITIES = {
    "Bogotá": ("BOG", 0.35, 14.0, 5.0),
    "Medellín": ("MED", 0.20, 22.0, 5.0),
    "Cali": ("CLO", 0.15, 24.5, 5.0),
    "Barranquilla": ("BAQ", 0.12, 28.0, 4.0),
    "Bucaramanga": ("BGA", 0.08, 23.0, 4.0),
    "Cartagena": ("CTG", 0.10, 28.0, 3.0),
}
FEEDERS_PER_CITY = 4
CUSTOMER_TYPES = {"residencial": 0.80, "comercial": 0.15, "industrial": 0.05}
NOMINAL_VOLTAGE = {"residencial": 120, "comercial": 208, "industrial": 440}
STRATUM_KWH_DAY = {1: 4.0, 2: 5.0, 3: 6.5, 4: 8.0, 5: 10.0, 6: 13.0}

# Forma del consumo a lo largo del día (24 horas). Se normaliza a media 1.
PROFILES = {
    "residencial": [
        0.5, 0.4, 0.35, 0.35, 0.4, 0.7, 1.2, 1.3, 1.0, 0.8, 0.8, 0.9, 1.0, 0.9, 0.8, 0.8, 0.9, 1.2, 1.7, 1.9,
        1.8, 1.5, 1.1, 0.7
    ],
    "comercial": [
        0.3, 0.3, 0.3, 0.3, 0.3, 0.4, 0.6, 1.0, 1.5, 1.7, 1.8, 1.8, 1.7, 1.7, 1.8, 1.8, 1.7, 1.5, 1.2, 0.9,
        0.6, 0.4, 0.3, 0.3
    ],
    "industrial": [
        0.6, 0.6, 0.6, 0.6, 0.6, 0.8, 1.3, 1.4, 1.4, 1.4, 1.4, 1.3, 1.2, 1.4, 1.4, 1.4, 1.4, 1.3, 1.1, 1.0,
        0.9, 0.8, 0.7, 0.6
    ],
}
# Factor de sábado y domingo
WEEKEND = {"residencial": (1.05, 1.10), "comercial": (0.70, 0.35), "industrial": (0.60, 0.40)}
# kWh adicionales por cada grado sobre 24 °C (aire acondicionado)
COOLING = {"residencial": 0.07, "comercial": 0.05, "industrial": 0.01}

# Eventos de la red durante el mes
OUTAGE = {"feeder": "BAQ-03", "day": date(2026, 9, 17), "slots": range(56, 68)}  # 14:00 a 17:00, sin servicio
UNDERVOLTAGE_FEEDER = "CLO-02"  # circuito sobrecargado: la tensión cae en la punta de la noche
METER_COLUMNS = ["meter_id", "customer_type", "stratum", "city", "feeder", "contracted_kw", "installed_at"]


def _build_meters():
    rng = random.Random(SEED)
    meters = []
    for i in range(1, METERS + 1):
        customer_type = rng.choices(list(CUSTOMER_TYPES), weights=list(CUSTOMER_TYPES.values()))[0]
        city = rng.choices(list(CITIES), weights=[c[1] for c in CITIES.values()])[0]
        if customer_type == "residencial":
            stratum = rng.choices(range(1, 7), weights=[20, 30, 25, 12, 8, 5])[0]
            kwh_day = STRATUM_KWH_DAY[stratum] * rng.lognormvariate(0, 0.25)
        elif customer_type == "comercial":
            stratum, kwh_day = None, rng.uniform(40, 150)
        else:
            stratum, kwh_day = None, rng.uniform(800, 3000)
        meters.append({
            "meter_id": f"M{i:05d}",
            "customer_type": customer_type,
            "stratum": stratum,
            "city": city,
            "feeder": f"{CITIES[city][0]}-{rng.randint(1, FEEDERS_PER_CITY):02d}",
            "contracted_kw": float(2 + stratum) if stratum else round(kwh_day / 24 * 3, 1),
            "installed_at": (date(2018, 1, 1) + timedelta(days=rng.randint(0, 2900))).isoformat(),
            "kwh_day": kwh_day,
        })
    return meters


def _build_anomalies(meters):
    rng = random.Random(SEED + 1)
    candidates = [m["meter_id"] for m in meters if m["customer_type"] != "industrial"]
    fraud_ids = rng.sample(candidates, 40)
    # Fraude (pérdida no técnica): desde un día del mes, el medidor registra entre el 30 % y el 60 % de lo real
    fraud = {m: (rng.randint(5, 22), rng.uniform(0.3, 0.6)) for m in fraud_ids}
    # Medidor dañado: desde un día del mes repite siempre la misma lectura
    stuck_ids = rng.sample([m for m in candidates if m not in fraud], 5)
    stuck = {m: rng.randint(8, 25) for m in stuck_ids}
    return fraud, stuck


METER_LIST = _build_meters()
FRAUD, STUCK = _build_anomalies(METER_LIST)


def meters():
    return [{k: m[k] for k in METER_COLUMNS} for m in METER_LIST]


def days():
    return [START + timedelta(days=d) for d in range(DAYS)]


def _weather_grid():
    rng = np.random.default_rng(SEED)
    hours = np.arange(24)
    grid = {}
    for city, (_, _, mean, amplitude) in CITIES.items():
        # Cada día es algo más frío o más caliente que el anterior (un paseo aleatorio suave)
        offsets = np.cumsum(rng.normal(0, 0.8, DAYS)) * 0.6
        daily = mean + amplitude * np.sin(2 * np.pi * (hours - 9) / 24)
        grid[city] = np.round(daily[None, :] + offsets[:, None] + rng.normal(0, 0.4, (DAYS, 24)), 1)
    return grid


WEATHER = _weather_grid()


def weather():
    for city in CITIES:
        for d, day in enumerate(days()):
            for hour in range(24):
                yield city, datetime.combine(
                    day, datetime.min.time()) + timedelta(hours=hour), WEATHER[city][d, hour]


def day_readings(day):
    """Lecturas de un día: arrays (medidores x 96) de kWh y voltaje, sin errores de calidad de datos."""
    d = (day - START).days
    rng = np.random.default_rng([SEED, d])
    hour = np.arange(SLOTS) // 4
    weekday = day.weekday()

    kwh = np.empty((METERS, SLOTS))
    voltage = np.empty((METERS, SLOTS))
    for i, m in enumerate(METER_LIST):
        kind = m["customer_type"]
        profile = np.array(PROFILES[kind])
        profile = profile / profile.mean()
        factor = WEEKEND[kind][weekday - 5] if weekday >= 5 else 1.0
        temperature = WEATHER[m["city"]][d, hour]
        cooling = 1 + COOLING[kind] * np.clip(temperature - 24, 0, None)
        kwh[i] = m["kwh_day"] / SLOTS * profile[hour] * factor * cooling
        nominal = NOMINAL_VOLTAGE[kind]
        voltage[i] = nominal * (1 + rng.normal(0, 0.008, SLOTS))
        if m["feeder"] == UNDERVOLTAGE_FEEDER:
            voltage[i, 72:88] *= 0.92  # 18:00 a 22:00
    kwh *= rng.lognormal(0, 0.12, kwh.shape)

    for i, m in enumerate(METER_LIST):
        if m["meter_id"] in FRAUD and d >= FRAUD[m["meter_id"]][0]:
            kwh[i] *= FRAUD[m["meter_id"]][1]
        if m["meter_id"] in STUCK and d >= STUCK[m["meter_id"]]:
            kwh[i] = round(m["kwh_day"] / SLOTS, 3)
        if day == OUTAGE["day"] and m["feeder"] == OUTAGE["feeder"]:
            kwh[i, OUTAGE["slots"]] = 0
            voltage[i, OUTAGE["slots"]] = 0
    return np.round(kwh, 3), np.round(voltage, 1)


def slot_time(day, slot):
    return datetime.combine(day, datetime.min.time()) + timedelta(minutes=15 * slot)


def readings_csv(day):
    """Un día de lecturas como llegaría del sistema de medición: con faltantes, duplicados y errores."""
    kwh, voltage = day_readings(day)
    rng = np.random.default_rng([SEED, (day - START).days, 1])
    missing = rng.random(kwh.shape) < 0.005  # lecturas que nunca llegaron
    duplicated = rng.random(kwh.shape) < 0.003  # reenviadas dos veces
    negative = rng.random(kwh.shape) < 0.0005  # error de signo en el concentrador
    no_voltage = rng.random(kwh.shape) < 0.001  # campo vacío
    times = [slot_time(day, s).strftime("%Y-%m-%d %H:%M:%S") for s in range(SLOTS)]

    lines = ["meter_id,ts,kwh,voltage"]
    for s in range(SLOTS):
        for i, m in enumerate(METER_LIST):
            if missing[i, s]:
                continue
            value = -kwh[i, s] if negative[i, s] else kwh[i, s]
            volts = "" if no_voltage[i, s] else f"{voltage[i, s]:.1f}"
            line = f"{m['meter_id']},{times[s]},{value:.3f},{volts}"
            lines.append(line)
            if duplicated[i, s]:
                lines.append(line)
    return "\n".join(lines) + "\n"


def inspections():
    """300 visitas de campo en octubre: 30 de los 40 fraudes, 3 de los 5 medidores dañados y 267 normales."""
    rng = random.Random(SEED + 2)
    fraud = rng.sample(sorted(FRAUD), 30)
    stuck = rng.sample(sorted(STUCK), 3)
    normal = rng.sample([m["meter_id"] for m in METER_LIST if m["meter_id"] not in set(FRAUD) | set(STUCK)],
                        267)
    rows = [(m, "fraude") for m in fraud] + [(m, "medidor dañado") for m in stuck] + [(m, "normal")
                                                                                      for m in normal]
    rng.shuffle(rows)
    return [(m, (date(2026, 10, 1) + timedelta(days=rng.randint(0, 9))).isoformat(), result)
            for m, result in rows]


def epoch_ms(ts):
    """'2026-09-30 18:00:00' (hora de Colombia) -> milisegundos desde 1970, como los timestamps de Kafka."""
    return int(datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ).timestamp() * 1000)
