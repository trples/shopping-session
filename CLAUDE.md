PROBLEM STATEMENT:
An online shop logs page views, add-to-carts, purchases, and ad clicks as CSV files. The logs are dirty, with duplicates, malformed lines, bad timestamps, and late events. One shopper can have many user IDs, and a third file links them across long chains and shared devices.

Clean the logs in /data, join each person's IDs, build sessions, credit each session to the ad click that brought the shopper in, count assisting campaigns from the week before, and write two daily Parquet tables (/output/user_daily/ and /output/campaign_daily/), one per person and one per campaign. The rules are exact so there is one right answer, and a fast answer that is almost right scores nothing.

Your score is container run time in seconds, and lower is better. The final runs on about 60 million events and caps each run at 3 minutes.

WHAT WE ARE CREATING:
Submit a public GitHub repository with a Dockerfile at its root and the commit to score. Keep the repository at most 50 MB with no data checked in, because the dataset is mounted at run time.

CONTAINER LIMITS:
Your image must be linux/amd64 and at most 8 GB. The build has network access for up to 10 minutes; runs have no network.
Each run gets 8 pinned vCPUs (hyperthreads), 24 GB of memory, and no swap.
/data is read-only, and /scratch is writable disk for spills. /output and /scratch together may hold at most 50 GB.
The root filesystem is read-only except for /tmp (64 MB in memory), and your process runs as uid 1000.
Environment variables: DATA_DIR=/data, OUTPUT_DIR=/output, SCRATCH_DIR=/scratch, HOME=/scratch, NCPU=8, and MEM_GB=24.
At most 4,096 processes and threads, and at most 50 MB on stdout (log to stderr).

---

## Working notes

- The exact rules are the organizers' `rules.md` (not checked in here). They win over everything else;
  `fixtures/README.md` summarizes the edge cases.
- `pipeline/pipeline.py` is the whole solution (DuckDB). The `Dockerfile` copies only that file.
- Correctness checks, all of which must pass before any speed change is kept:
  - `python3 scripts/run_fixtures.py fixtures -- python3 pipeline/pipeline.py` (36/36 edge cases)
  - `scripts/gen.py` (an approximate local generator) plus `scripts/reference.py` (an independent
    pure-Python implementation), diffed with `scripts/compare.py OUT REF`.
- DuckDB's CSV reader treats `\r` as a line break. Files containing `\r` (or `\x01`, the line
  reader's delimiter) go through a byte-exact `read_text` path instead.
