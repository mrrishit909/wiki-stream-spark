"""Step 5: what the stream showed, from Spark's outputs (and lateness from the recording).

    ./venv/bin/python summarize.py   -> results/per_minute.csv, per_wiki_top.csv, bursts.csv, lateness.csv, summary.csv
"""
from pathlib import Path

import duckdb

HERE = Path(__file__).parent
R = HERE / "results"


def main():
    c = duckdb.connect()
    out = lambda name, sql: c.execute(f"COPY ({sql}) TO '{R / (name + '.csv')}' (HEADER)")
    out("per_minute", f"SELECT * FROM '{HERE}/data/out/per_minute/*.parquet' ORDER BY minute, type")
    out("per_wiki_top", f"""SELECT wiki, sum(changes) AS changes FROM '{HERE}/data/out/per_wiki/*.parquet' GROUP BY 1 ORDER BY 2 DESC LIMIT 12""")
    # user (2) and user talk (3) pages are named after a person: publish the namespace, not the title
    out("bursts", f"""SELECT window_start, wiki, namespace, CASE WHEN namespace IN (2, 3) THEN '(user or user talk page)' ELSE title END AS title,
                             edits, editors, reverts, anonymous_edits FROM '{HERE}/data/out/bursts/*.parquet' ORDER BY edits DESC""")
    c.execute(f"""CREATE TABLE ev AS SELECT *, CAST(event_time AS TIMESTAMP) AS et, regexp_extract(filename, 'part-(\\d+)', 1)::int AS f
                    FROM read_json('{HERE}/data/landing/part-*.json', format = 'newline_delimited', filename = true,
                                   columns = {{type: 'VARCHAR', title: 'VARCHAR', wiki: 'VARCHAR', bot: 'BOOLEAN', anonymous: 'BOOLEAN',
                                              revert: 'BOOLEAN', user_hash: 'VARCHAR', event_time: 'VARCHAR'}})""")
    # how far behind the newest event already delivered each event arrived (file order = arrival order)
    out("lateness", """WITH m AS (SELECT f, max(et) AS fmax FROM ev GROUP BY f),
                            r AS (SELECT f, max(fmax) OVER (ORDER BY f ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seen FROM m)
                       SELECT count(*) AS events_after_first_file, max(date_diff('second', e.et, r.seen)) AS worst_seconds_behind,
                              count(*) FILTER (WHERE date_diff('second', e.et, r.seen) > 120) AS more_than_2_minutes_behind
                         FROM ev e JOIN r USING (f) WHERE r.seen IS NOT NULL""")
    out("summary", """SELECT count(*) AS events, count(DISTINCT wiki) AS wikis, round(100 * avg(bot::int), 1) AS pct_bot,
                             round(100 * avg(anonymous::int), 1) AS pct_anonymous, round(100 * avg(revert::int), 2) AS pct_revert,
                             count(*) FILTER (WHERE type = 'edit') AS edits, count(*) FILTER (WHERE type = 'new') AS new_pages,
                             count(*) FILTER (WHERE type = 'categorize') AS categorize, count(*) FILTER (WHERE type = 'log') AS log_events,
                             min(et) AS first_event, max(et) AS last_event FROM ev""")
    for f in ["summary", "lateness", "bursts", "per_wiki_top"]:
        print(f"== {f}\n", c.execute(f"SELECT * FROM '{R / (f + '.csv')}'").df().to_string(index=False))


if __name__ == "__main__":
    main()
