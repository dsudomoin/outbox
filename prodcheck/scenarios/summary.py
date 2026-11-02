#!/usr/bin/env python3
"""summary.py [SCENARIO...]: one block per scenario from build/results, the failed checks and the numbers."""
import glob, json, os, sys
here = os.path.dirname(os.path.abspath(__file__))
names = sys.argv[1:] or sorted(os.path.basename(f)[:-5] for f in glob.glob(f"{here}/../build/results/*.json"))
for n in names:
    d = json.load(open(f"{here}/../build/results/{n}.json"))
    l = d["ledger"]
    print(d["scenario"], d["verdict"], f'{d.get("seconds")}s', d.get("repaired"), {k: l.get(k) for k in (
        "sent", "effects", "dead", "discarded", "lost", "duplicates", "latency_p99", "latency_max", "drain_seconds", "violations")})
    for c in d["checks"]:
        if "НЕ ПРОЙДЕНА" in c:
            print("   ", c)
