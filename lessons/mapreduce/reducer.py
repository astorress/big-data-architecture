# Reducer de Hadoop Streaming: recibe las líneas ordenadas por clave y suma los valores de cada clave.
# Emite el mismo formato que recibe, así que también sirve de combiner (una reducción parcial en cada map).
import sys

current, kwh, readings, invalid = None, 0.0, 0, 0
for line in sys.stdin:
    key, line_kwh, line_readings, line_invalid = line.rstrip("\n").split("\t")
    if key != current:
        if current is not None:
            print(f"{current}\t{kwh:.3f}\t{readings}\t{invalid}")
        current, kwh, readings, invalid = key, 0.0, 0, 0
    kwh += float(line_kwh)
    readings += int(line_readings)
    invalid += int(line_invalid)
if current is not None:
    print(f"{current}\t{kwh:.3f}\t{readings}\t{invalid}")
