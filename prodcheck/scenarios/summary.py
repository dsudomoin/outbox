#!/usr/bin/env python3
"""summary.py [SCENARIO...]: one block per scenario from RESULT_DIR (default build/results, build/results-pg17 with
PG_VERSION=17), the failed checks and the numbers."""
import glob, json, os, sys
here = os.path.dirname(os.path.abspath(__file__))
pg = os.environ.get("PG_VERSION", "16")
base = os.environ.get("RESULT_DIR") or f"{here}/../build/results" + ("" if pg == "16" else f"-pg{pg}")
names = sys.argv[1:] or sorted(os.path.basename(f)[:-5] for f in glob.glob(f"{base}/*.json"))
for n in names:
    d = json.load(open(f"{base}/{n}.json"))
    l = d["ledger"]
    print(d["scenario"], d["verdict"], f'{d.get("seconds")}s', d.get("repaired"), {k: l.get(k) for k in (
        "sent", "effects", "dead", "discarded", "lost", "duplicates", "latency_p99", "latency_max", "drain_seconds", "violations")})
    for c in d["checks"]:
        if "НЕ ПРОЙДЕНА" in c:
            print("   ", c)
