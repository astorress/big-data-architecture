# Mapper de Hadoop Streaming: lee líneas CSV por stdin y emite "clave<TAB>valores" por stdout.
# Clave: el día. Valores: kWh válidos, 1 lectura y 1 si la lectura es inválida (kWh negativo o voltaje vacío).
import sys

for line in sys.stdin:
    meter_id, ts, kwh, voltage = line.rstrip("\n").split(",")
    if meter_id == "meter_id":  # encabezado (cada archivo trae el suyo)
        continue
    kwh = float(kwh)
    invalid = kwh < 0 or voltage == ""
    print(f"{ts[:10]}\t{0.0 if invalid else kwh}\t1\t{int(invalid)}")
