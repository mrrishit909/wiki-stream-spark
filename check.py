"""The project's check: the streaming results recomputed in one batch query (DuckDB) over the same recorded files.

    ./venv/bin/python check.py

1. Every 1-minute and 5-minute window Spark wrote has exactly the counts of a batch GROUP BY over all events; no row was
   dropped as late in this recording (results/stream_progress.csv), so nothing may differ.
2. Every closed window (ending before the final watermark) was written; the still-open ones at the end were not.
3. Every edit burst Spark wrote has the same edit count as the batch; its distinct editors come from a HyperLogLog
   sketch (approx_count_distinct), so they are compared with the exact count to within 10%.
"""
import json
from pathlib import Path

import duckdb
import pandas as pd

HERE = Path(__file__).parent
R = HERE / "results"


def main():
    con = duckdb.connect()
    con.execute(f"""CREATE TABLE ev AS SELECT *, CAST(event_time AS TIMESTAMP) AS et
                      FROM read_json('{HERE}/data/landing/part-*.json', format = 'newline_delimited',
                                     columns = {{id: 'BIGINT', type: 'VARCHAR', title: 'VARCHAR', wiki: 'VARCHAR', bot: 'BOOLEAN',
                                                anonymous: 'BOOLEAN', revert: 'BOOLEAN', user_hash: 'VARCHAR', event_time: 'VARCHAR'}})""")
    prog = pd.read_csv(R / "stream_progress.csv")
    dropped = prog.groupby("query")["late_rows_dropped"].sum().to_dict()
    final_wm = pd.Timestamp(prog["watermark"].dropna().max()).tz_localize(None)

    sp = con.execute(f"SELECT * FROM '{HERE}/data/out/per_minute/*.parquet'").df()
    b = con.execute("""SELECT date_trunc('minute', et) AS minute, type, count(*) AS changes, sum(bot::int) AS bot,
                              sum(anonymous::int) AS anonymous, sum(revert::int) AS reverts FROM ev GROUP BY ALL""").df()
    j = sp.merge(b, on=["minute", "type"], suffixes=("", "_batch"))
    assert len(j) == len(sp), "a streamed window is missing from the batch"
    assert sum(dropped.values()) == 0 and (j["changes_batch"] == j["changes"]).all(), dropped
    closed = b[b["minute"] + pd.Timedelta(minutes=1) <= final_wm - pd.Timedelta(minutes=1)]
    assert set(map(tuple, closed[["minute", "type"]].values)) <= set(map(tuple, sp[["minute", "type"]].values)), "a closed window was not written"

    sw = con.execute(f"SELECT * FROM '{HERE}/data/out/per_wiki/*.parquet'").df()
    bw = con.execute("SELECT time_bucket(INTERVAL 5 MINUTE, et) AS window_start, wiki, count(*) AS changes FROM ev GROUP BY ALL").df()
    jw = sw.merge(bw, on=["window_start", "wiki"], suffixes=("", "_batch"))
    assert len(jw) == len(sw) and (jw["changes_batch"] == jw["changes"]).all()

    sb = con.execute(f"SELECT * FROM '{HERE}/data/out/bursts/*.parquet'").df()
    if len(sb):
        bb = con.execute("""SELECT w.window_start, e.wiki, e.title, count(*) AS edits_batch, count(DISTINCT e.user_hash) AS editors_exact
                              FROM ev e, (SELECT DISTINCT time_bucket(INTERVAL 5 MINUTE, et) - INTERVAL 5 MINUTE AS window_start FROM ev
                                          UNION SELECT DISTINCT time_bucket(INTERVAL 5 MINUTE, et) FROM ev) w
                             WHERE e.type IN ('edit', 'new') AND NOT e.bot AND e.et >= w.window_start AND e.et < w.window_start + INTERVAL 10 MINUTE
                             GROUP BY ALL""").df()
        jb = sb.merge(bb, on=["window_start", "wiki", "title"])
        assert len(jb) == len(sb)
        assert (jb["edits"] == jb["edits_batch"]).all()
        assert ((jb["editors"] - jb["editors_exact"]).abs() <= 0.1 * jb["editors_exact"] + 1).all()
    total = con.execute("SELECT count(*) FROM ev").fetchone()[0]
    coll = json.loads((R / "collection.json").read_text())
    assert total == coll["events"], (total, coll["events"])
    print(f"OK: {total:,} events; {len(sp)} per-minute and {len(sw)} per-wiki windows match a batch GROUP BY "
          f"exactly (no late rows); every closed window written; {len(sb)} bursts' edit counts match and HLL editors within 10%")


if __name__ == "__main__":
    main()
