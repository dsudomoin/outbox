#!/usr/bin/env python3
"""soak_report.py SERIES: from build/results/soak.series, one line per finding, "0|text" or "1|text"; exit 1 on a "1".
Retry and parked tables must be back in the order of size they had in the first third of the run (10 times, at least 1 MB
of room: an empty table's pages); the rest is reported, what the outbox's data does over time."""
import csv, statistics, sys


def main(path):
    rows = list(csv.DictReader(open(path), delimiter="\t"))
    third, rest = rows[:max(1, len(rows) // 3)], rows[-max(1, len(rows) // 3):]

    def col(name, part=rows):
        return [float(r[name]) for r in part if r[name] not in ("-", "", None)]

    bad = 0
    for name in ("shop_retry_mb", "shop_parked_mb", "acc_retry_mb", "acc_parked_mb"):
        values, first = col(name), col(name, third)
        start = statistics.median(first) if first else 0.0
        now = values[-1]
        fail = now > 10 * max(start, 1)
        bad += fail
        print(f"{int(fail)}|{name}: в первой трети {start} МБ, пик {max(values)}, в конце {now}")
    for name in ("db_mb", "shop_mb", "acc_mb", "shop_parts", "acc_parts", "shop_dead", "acc_dead", "shop_dead_tup", "acc_dead_tup"):
        values = col(name)
        print(f"0|{name}: начало {values[0]}, пик {max(values)}, конец {values[-1]}")
    heaps = [n for n in rows[0] if n.startswith("heap_")]

    def top(part):
        tops = [max(float(r[n]) for n in heaps if r[n] != "-") for r in part if any(r[n] != "-" for n in heaps)]
        return statistics.median(tops) if tops else 0

    print(f"0|старое поколение, наибольшее из экземпляров, медиана: первая треть {top(third)} МБ, последняя {top(rest)}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
