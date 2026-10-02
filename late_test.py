"""Step 4: does the watermark do its job? Replay 500 old events after everything else and see what Spark does.

    ./venv/bin/python late_test.py   -> results/late_test.csv

The recorded stream had no event more than 2 minutes behind (results/lateness.csv), so the watermark never had to act.
This copies the recording, adds one more file holding 500 events from early in the recording (as a lagging source or
a re-delivery would), runs the same streaming job, and compares it with the normal run. With a 2-minute watermark in
append mode, Spark should drop exactly those 500 rows and leave every written window unchanged.
"""
import os
import shutil
import subprocess
from pathlib import Path

import duckdb
import pandas as pd

HERE = Path(__file__).parent
SRC, LATE = HERE / "data" / "landing", HERE / "data" / "landing_late"
N = 500


def main():
    shutil.rmtree(LATE, ignore_errors=True)
    shutil.copytree(SRC, LATE)
    first = sorted(SRC.glob("part-*.json"))[10]
    lines = first.read_text().splitlines()[:N]
    last = max(int(p.stem.split("-")[1]) for p in SRC.glob("part-*.json"))
    (LATE / f"part-{last + 1:05d}.json").write_text("\n".join(lines) + "\n")
    env = dict(os.environ, LANDING=str(LATE), OUTPUT=str(HERE / "data" / "out_late"), PROGRESS="stream_progress_late.csv")
    subprocess.run([str(HERE / "venv" / "bin" / "python"), str(HERE / "stream.py")], env=env, check=True, capture_output=True)
    prog = pd.read_csv(HERE / "results" / "stream_progress_late.csv")
    c = duckdb.connect()
    rows = []
    for q in ["per_minute", "per_wiki"]:
        normal = c.execute(f"SELECT * FROM '{HERE}/data/out/{q}/*.parquet' ORDER BY ALL").df()
        late = c.execute(f"SELECT * FROM '{HERE}/data/out_late/{q}/*.parquet' ORDER BY ALL").df()
        rows.append({"query": q, "late_rows_sent": N, "rows_dropped_by_watermark": int(prog.loc[prog["query"] == q, "late_rows_dropped"].sum()),
                     "windows_written_normal_run": len(normal), "windows_written_late_run": len(late),
                     "written_windows_identical": normal.equals(late)})
    # Spark counts dropped rows after the partial (map-side) aggregation, so 500 late events show up as the number of
    # (window, key) groups they fall into, not as 500. Count those groups straight from the replayed lines.
    lf = LATE / f"part-{last + 1:05d}.json"
    groups = {"per_minute": c.execute(f"SELECT count(*) FROM (SELECT DISTINCT date_trunc('minute', CAST(event_time AS TIMESTAMP)), type FROM read_json('{lf}', format = 'newline_delimited'))").fetchone()[0],
              "per_wiki": c.execute(f"SELECT count(*) FROM (SELECT DISTINCT time_bucket(INTERVAL 5 MINUTE, CAST(event_time AS TIMESTAMP)), wiki FROM read_json('{lf}', format = 'newline_delimited'))").fetchone()[0]}
    behind = c.execute(f"""SELECT date_diff('second', (SELECT max(CAST(event_time AS TIMESTAMP)) FROM read_json('{lf}', format = 'newline_delimited')),
                                                   (SELECT max(CAST(event_time AS TIMESTAMP)) FROM read_json('{SRC}/part-*.json', format = 'newline_delimited'))) / 60.0""").fetchone()[0]
    for r in rows:
        r["partial_groups_in_late_rows"] = groups[r["query"]]
        r["late_rows_minutes_behind"] = round(behind, 1)
        assert r["rows_dropped_by_watermark"] == r["partial_groups_in_late_rows"] and r["written_windows_identical"], r
    out = pd.DataFrame(rows)
    out.to_csv(HERE / "results" / "late_test.csv", index=False)
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
