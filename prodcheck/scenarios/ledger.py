#!/usr/bin/env python3
"""The machine check of one scenario run. Waits until the run is drained, then compares what the writers committed
(pc_sent, written in the transaction of the send) with what the sinks saw (pc_arrival, every arrival) and did
(pc_effect, the effect, atomic with its dedupe). Prints JSON; exit 0 clean, 1 a violation, 2 not drained."""
import argparse, bisect, csv, io, json, subprocess, sys, time

p = argparse.ArgumentParser()
p.add_argument("--source", default="kora-outbox-prodcheck-source")
p.add_argument("--target", default="kora-outbox-prodcheck-target", help="empty: this outbox's sinks are all on the source")
p.add_argument("--base", action="append", default=[], help="outbox base table, repeatable")
p.add_argument("--timeout", type=int, default=240)
p.add_argument("--dead-tags", default="", help="tags that may end in the dead table")
p.add_argument("--discard-tags", default="", help="tags removed on purpose: neither effect nor dead letter")
p.add_argument("--discarded-file", help="message ids discarded through the admin API, one per line")
p.add_argument("--exempt-tags", default="", help="tags the order checks skip: a dead letter, requeue, redrive")
p.add_argument("--exempt-topics", default="", help="topics the order checks skip: redrive by filter, restored rows")
p.add_argument("--order-within-tag", action="append", default=[], help="exempt from the key order, but in order among themselves")
p.add_argument("--report-order-within-tag", action="append", default=[], help="the same, counted and reported, no violation")
p.add_argument("--late-block", action="append", default=[], metavar="KEY=MAX",
               help="a key larger than a call: its order may break by one late block of at most MAX overtaking rows, nothing more")
p.add_argument("--search-path", help="schema of the ledger tables, when not public")
p.add_argument("--max-dups", type=int, help="arrivals beyond the first of a message, at most")
p.add_argument("--min-dups", type=int, default=0, help="at least: the fault really hit a batch in flight")
p.add_argument("--allow-phantoms", action="store_true", help="restore or failover: arrivals of lost commits are allowed")
p.add_argument("--no-time-checks", action="store_true", help="the clock itself is the fault: skip checks by timestamps")
p.add_argument("--allow-left", default="", help="comma list of retry,parked,dead-pending allowed to stay non-zero")
p.add_argument("--p99-max", type=float, help="seconds, delivery latency of messages outside --slow-keys and --slow-tags")
p.add_argument("--slow-keys", default="")
p.add_argument("--slow-tags", default="")
p.add_argument("--slow-from", type=float, help="latency check only for messages sent after this unix time")
p.add_argument("--slow-to", type=float, help="and before this one")
args = p.parse_args()
split = lambda s: {x for x in s.split(",") if x}
dead_tags, discard_tags, exempt = split(args.dead_tags), split(args.discard_tags), split(args.exempt_tags)
exempt_topics = split(args.exempt_topics)
allow_left = split(args.allow_left)
late_keys = {k: int(v) for k, v in (x.split("=") for x in args.late_block)}


def rows(container, sql):
    env = ["-e", f"PGOPTIONS=-c search_path={args.search_path}"] if args.search_path else []
    out = subprocess.run(["docker", "exec", *env, container, "psql", "-U", "outbox", "-d", "outbox", "-Atq", "-c",
                          f"copy ({sql}) to stdout with csv"], capture_output=True, text=True, check=True).stdout
    return list(csv.reader(io.StringIO(out)))


def one(container, sql):
    return int(rows(container, sql)[0][0])


def left():
    """What still waits: (undelivered, retry, parked, pending or scheduled dead letters)."""
    sent = one(args.source, "select count(*) from pc_sent")
    done = sum(one(c, "select count(*) from pc_effect") for c in (args.source, args.target) if c)
    dead = retry = parked = pending = 0
    for b in args.base:
        dead += one(args.source, f"select count(*) from {b}_dead")
        retry += one(args.source, f"select count(*) from {b}_retry")
        parked += one(args.source, f"select count(*) from {b}_parked")
        pending += one(args.source, f"select count(*) from {b}_dead where status <> 'kept'")
    return sent - done - dead - discarded_count, retry, parked, pending


discarded = set(open(args.discarded_file).read().split()) if args.discarded_file else set()
discarded_count = len(discarded)
result = {"violations": []}
deadline = time.time() + args.timeout
started, prev, drained = time.time(), None, False
while time.time() < deadline:
    try:
        cur = left()
    except subprocess.CalledProcessError as e:
        cur = str(e.stderr)
    # A tag removed by a hook has no row left, so its messages keep `undelivered` above 0: counted in the final check.
    ready = isinstance(cur, tuple) and (cur[0] <= 0 or discard_tags) \
        and (cur[1] == 0 or "retry" in allow_left) and (cur[2] == 0 or "parked" in allow_left) \
        and (cur[3] == 0 or "dead-pending" in allow_left)
    if ready and cur == prev:
        drained = True
        break
    prev = cur if ready else None
    time.sleep(3)
result["drain_seconds"] = round(time.time() - started, 1)
result["left_at_end"] = prev if drained else cur

sent = {r[0]: {"outbox": r[1], "topic": r[2], "key": r[3], "seq": int(r[4]), "tag": r[5], "contract": int(r[6]), "at": float(r[7])}
        for r in rows(args.source, "select message_id, outbox, topic, key, seq, tag, contract, extract(epoch from at) from pc_sent")}
rolled = {r[0] for r in rows(args.source, "select message_id from pc_rolled_back")}
arrivals, effects = [], []
for db, c in (("source", args.source), ("target", args.target)):
    if not c:
        continue
    arrivals += [{"db": db, "no": int(r[0]), "sink": r[1], "id": r[2], "at": float(r[3]), "xmin": r[4]} for r in rows(
        c, "select no, sink, message_id, extract(epoch from at), xmin from pc_arrival order by no")]
    effects += [{"db": db, "no": int(r[0]), "sink": r[1], "id": r[2], "at": float(r[3]), "xmin": r[4]} for r in rows(
        c, "select no, sink, message_id, extract(epoch from at), xmin from pc_effect order by no")]
dead = {}
for b in args.base:
    for r in rows(args.source, f"select message_id, status, attempts, error, redrives from {b}_dead"):
        dead[r[0]] = {"base": b, "status": r[1], "attempts": int(r[2]), "error": r[3], "redrives": int(r[4])}

effect_ids = {e["id"] for e in effects}
arrival_ids = {a["id"] for a in arrivals}
discarded_ids = discarded | {i for i, m in sent.items() if m["tag"] in discard_tags and i not in dead and i not in effect_ids}
lost = [i for i in sent if i not in effect_ids and i not in dead and i not in discarded_ids]
phantom = sorted(rolled & (arrival_ids | effect_ids))
unknown = sorted((arrival_ids | effect_ids) - sent.keys() - rolled)
bad_dead = sorted(i for i in dead if i in sent and sent[i]["tag"] not in dead_tags)
dead_and_effect = sorted(set(dead) & effect_ids)

seen = {}
for e in effects:
    seen[(e["sink"], e["id"])] = seen.get((e["sink"], e["id"]), 0) + 1
double_effects = sum(1 for v in seen.values() if v > 1)
wrong_sink = 0


def by_key(events, only_tag=None):
    """Events of known, non-exempt messages per (sink, topic, key), in their sink's order, first event of a message.
    With only_tag: the messages of that tag alone, exempt or not."""
    keys, firsts = {}, set()
    for ev in sorted(events, key=lambda x: (x["db"], x["no"])):
        m = sent.get(ev["id"])
        skip = m["tag"] != only_tag if m and only_tag else m and (m["tag"] in exempt or m["topic"] in exempt_topics)
        if not m or m["key"] == "" or m["seq"] == 0 or skip or (ev["sink"], ev["id"]) in firsts:
            continue
        firsts.add((ev["sink"], ev["id"]))
        keys.setdefault((ev["sink"], m["topic"], m["key"]), []).append({**ev, "seq": m["seq"]})
    return keys


def inversions(keys):
    bad = []
    for k, evs in keys.items():
        for a, b in zip(evs, evs[1:]):
            if b["seq"] <= a["seq"]:
                bad.append({"key": list(k), "seq": [a["seq"], b["seq"]], "ids": [a["id"], b["id"]]})
    return bad


def late_blocks(seqs):
    """Of a key's rows in their order, each block of rows lower than one already seen: (rows that overtook it, its rows)."""
    seen, blocks, i = [], [], 0
    while i < len(seqs):
        bisect.insort(seen, seqs[i])
        if seqs[i] != seen[-1]:
            over = len(seen) - bisect.bisect_right(seen, seqs[i])
            n = 1
            while i + 1 < len(seqs) and seqs[i + 1] < seen[-1]:
                i += 1
                bisect.insort(seen, seqs[i])
                n += 1
            blocks.append([over, n])
        i += 1
    return blocks


def strict(keys):
    return {k: evs for k, evs in keys.items() if k[2] not in late_keys}


effect_order = inversions(strict(by_key(effects)))
arrival_order = inversions(strict(by_key(arrivals)))
for tag in args.order_within_tag:
    effect_order += inversions(strict(by_key(effects, tag)))
    arrival_order += inversions(strict(by_key(arrivals, tag)))
# The handler must not get message n+1 of a key before message n is delivered.
overtakes = []
if not args.no_time_checks:
    first_effect, effect_tx = {}, {}
    for k, evs in by_key(effects).items():
        for ev in evs:
            first_effect[(k, ev["seq"])] = ev["at"]
            effect_tx[(k, ev["seq"])] = ev["xmin"]
    for k, evs in by_key(arrivals).items():
        for a, b in zip(evs, evs[1:]):
            done = first_effect.get((k, a["seq"]))
            if done is not None and b["at"] < done:
                overtakes.append({"key": list(k), "seq": [a["seq"], b["seq"]], "early_by": round(done - b["at"], 6),
                                  "arrival_tx": b["xmin"], "effect_tx": effect_tx[(k, a["seq"])], "arrival_at": b["at"], "effect_at": done})

late = {}
for name, events in (("effects", effects), ("first_arrivals", arrivals)):
    for k, evs in by_key(events).items():
        if k[2] in late_keys:
            late.setdefault(k[2], {})[name] = late_blocks([e["seq"] for e in evs])
result["late_blocks"] = late
result["order_within_tag"] = {tag: len(inversions(by_key(effects, tag))) for tag in args.report_order_within_tag}
dups = len(arrivals) - len({(a["sink"], a["id"]) for a in arrivals})
latency = []
slow_keys, slow_tags = split(args.slow_keys), split(args.slow_tags)
first_done = {}
for e in effects:
    first_done.setdefault(e["id"], e["at"])
for i, m in sent.items():
    if i in first_done and m["key"] not in slow_keys and m["tag"] not in slow_tags \
            and (args.slow_from is None or m["at"] >= args.slow_from) and (args.slow_to is None or m["at"] <= args.slow_to):
        latency.append(first_done[i] - m["at"])
latency.sort()
pct = lambda q: round(latency[min(int(len(latency) * q), len(latency) - 1)], 3) if latency else None

result.update(
    sent=len(sent), rolled_back=len(rolled), arrivals=len(arrivals), effects=len(effect_ids),
    dead=len(dead), dead_kept=sum(1 for d in dead.values() if d["status"] == "kept"), discarded=len(discarded_ids),
    lost=len(lost), lost_sample=lost[:5], phantom_rolled_back=len(phantom), phantom_sample=phantom[:5],
    unknown_arrivals=len(unknown), unknown_sample=unknown[:5], unexpected_dead=len(bad_dead), unexpected_dead_sample=bad_dead[:5],
    dead_and_effect=len(dead_and_effect), double_effects=double_effects, order_violations=len(effect_order),
    order_sample=effect_order[:3], first_arrival_violations=len(arrival_order), first_arrival_sample=arrival_order[:3],
    overtakes=len(overtakes), overtake_sample=overtakes[:3], duplicates=dups, latency_p50=pct(.5), latency_p99=pct(.99),
    latency_max=pct(1), latency_n=len(latency))
v = result["violations"]
if not drained:
    v.append(f"not drained in {args.timeout} s: left {result['left_at_end']}")
if lost:
    v.append(f"lost {len(lost)}")
if phantom:
    v.append(f"delivered from rolled-back transactions: {len(phantom)}")
if unknown and not args.allow_phantoms:
    v.append(f"arrivals of unknown messages: {len(unknown)}")
if bad_dead:
    v.append(f"unexpected dead letters: {len(bad_dead)}")
if dead_and_effect:
    v.append(f"both dead and delivered: {len(dead_and_effect)}")
if double_effects:
    v.append(f"double effects: {double_effects}")
if effect_order:
    v.append(f"effect order violations: {len(effect_order)}")
if arrival_order:
    v.append(f"first-arrival order violations: {len(arrival_order)}")
if overtakes:
    v.append(f"overtakes: {len(overtakes)}")
for key, limit in late_keys.items():
    for name, blocks in late.get(key, {}).items():
        if len(blocks) > 1 or any(over > limit for over, _ in blocks):
            v.append(f"{key}: {name} late blocks {blocks[:5]} (of {len(blocks)}), allowed one of at most {limit} overtaking rows")
if args.max_dups is not None and dups > args.max_dups:
    v.append(f"duplicates {dups} above the allowed {args.max_dups}")
if dups < args.min_dups:
    v.append(f"duplicates {dups} below {args.min_dups}: the fault did not hit a batch in flight")
if args.p99_max is not None and (not latency or latency[min(int(len(latency) * .99), len(latency) - 1)] > args.p99_max):
    v.append(f"p99 latency {pct(.99)} above {args.p99_max}")
if not sent:
    v.append("nothing was sent: inconclusive")
print(json.dumps(result, ensure_ascii=False))
sys.exit(2 if not drained else 1 if v else 0)
