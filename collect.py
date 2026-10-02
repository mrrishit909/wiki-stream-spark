"""Step 1: record Wikimedia's live recent-changes stream into small files, the way a message queue would hand them on.

    ./venv/bin/python collect.py 30    -> data/landing/part-<n>.json (one JSON event per line, a new file every 20 s)
                                          results/collection.json

Source: https://stream.wikimedia.org/v2/stream/recentchange (Server-Sent Events, every edit on every Wikimedia wiki).
Only the fields used later are kept; user names are dropped (aggregates only are published).
"""
import hashlib
import ipaddress
import json
import ssl
import sys
import time
import urllib.request
from pathlib import Path

import certifi

HERE = Path(__file__).parent
LAND = HERE / "data" / "landing"
URL = "https://stream.wikimedia.org/v2/stream/recentchange"
UA = "wiki-stream-spark/1.0 (portfolio project; https://github.com/mrrishit909/wiki-stream-spark)"
KEEP = ["id", "type", "namespace", "title", "wiki", "server_name", "bot", "minor", "patrolled", "timestamp"]


def anonymous(user):
    """An IP address, or one of the temporary accounts ("~2025-...") that replaced IP editing on many wikis."""
    if user.startswith("~"):
        return True
    try:
        ipaddress.ip_address(user)
        return True
    except ValueError:
        return False


def slim(e):
    out = {k: e.get(k) for k in KEEP}
    out["event_time"] = e.get("meta", {}).get("dt")                     # when the change happened (ISO, UTC)
    out["anonymous"] = anonymous(e.get("user") or "")
    length = e.get("length") or {}
    out["bytes_change"] = (length.get("new") or 0) - (length.get("old") or 0) if length else None
    out["revert"] = any(t in (e.get("comment") or "").lower() for t in ("revert", "undid", "rv "))
    out["user_hash"] = hashlib.sha1(e["user"].encode()).hexdigest()[:12] if e.get("user") else None   # count distinct editors without keeping names
    return out


def main(minutes):
    LAND.mkdir(parents=True, exist_ok=True)
    ctx = ssl.create_default_context(cafile=certifi.where())
    req = urllib.request.Request(URL, headers={"User-Agent": UA, "Accept": "text/event-stream"})
    start = time.time()
    part, buf, n, last_flush = 0, [], 0, time.time()
    while time.time() - start < minutes * 60:
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
                for raw in r:
                    line = raw.decode("utf-8", "replace").strip()
                    if line.startswith("data:"):
                        try:
                            e = json.loads(line[5:])
                        except json.JSONDecodeError:
                            continue
                        if e.get("meta", {}).get("domain") == "canary":
                            continue
                        buf.append(json.dumps(slim(e)))
                        n += 1
                    if time.time() - last_flush >= 20 and buf:
                        tmp = LAND / f".part-{part:05d}.json"                 # write then rename: a reader never sees half a file
                        tmp.write_text("\n".join(buf) + "\n")
                        tmp.rename(LAND / f"part-{part:05d}.json")
                        part, buf, last_flush = part + 1, [], time.time()
                    if time.time() - start >= minutes * 60:
                        break
        except Exception as ex:                                            # the stream drops now and then: reconnect
            print("reconnect after", type(ex).__name__, flush=True)
            time.sleep(2)
    if buf:
        (LAND / f"part-{part:05d}.json").write_text("\n".join(buf) + "\n")
        part += 1
    (HERE / "results").mkdir(exist_ok=True)
    (HERE / "results" / "collection.json").write_text(json.dumps({"source": URL, "minutes": minutes, "events": n, "files": part,
                                                                  "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start))}, indent=2) + "\n")
    print(n, "events in", part, "files")


if __name__ == "__main__":
    main(float(sys.argv[1]))
