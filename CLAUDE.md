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
  - Real dev check: build `generator.zip` (Rust 1.85+, `cargo build --release`), then
    `generate --seed 1 --events 1000000 --clicks 250000 --output dev`, unzip `dev-answer.zip`,
    run the pipeline into `out/` and `python3 scripts/compare.py out dev-answer` (MATCH as of 1ba653f).
    The real generator also makes a 60M set in ~7s (`--events 60000000 --clicks 15000000`): ~35s locally.
  - For scale: `scripts/gen.py` at 6M events, then replicate ×10 with distinct IDs to reach ~60M,
    and diff each speed change against a baseline output of an earlier, verified version.
- DuckDB's CSV reader treats `\r` as a line break. Files containing `\r` (or `\x02`, the fast
  reader's null string) go through a byte-exact `read_text` + regex path instead.
- Fast path: `read_csv` with `quote=''`, one extra sentinel column, `null_padding`, `strict_mode`,
  `ignore_errors`, `nullstr='\x02'`, so field counts are exact; quotes are checked per field.
- Current local times (8 threads, M-series Mac): public 1M dev set ~0.5s (MATCH); real 60M ~27s.
  The submission's first gate is the 1M dev set, so small-input overheads matter.
- Speed lessons:
  - Dedup: `QUALIFY row_number()` beats `arg_min(struct)` group-by by 2-3x.
  - Windows with frames (`ROWS UNBOUNDED PRECEDING`) over 54M rows are ~3x slower than
    frame-free `lag`/`lead`/`row_number`; sessionize with those only.
  - Macros inline their arguments at every mention: a macro over a computed expression
    recomputes it each time. Keep macro bodies short and arguments plain columns (or
    materialize first). This made timestamp parsing several times slower.
  - `try_strptime` is ~7x slower than `strptime`; `TRY_CAST(... AS TIMESTAMP)` is fast.
  - Independent steps run concurrently on separate cursors (`run_parallel`); DuckDB shares
    one thread pool, so small inputs stop leaving threads idle.
  - CREATE TABLE of a big intermediate costs ~4x a materialized CTE; avoid large tables.
  - Tried and rejected: replace-based quote stripping, whole-line regex on concat_ws,
    translate()-based timestamp shape checks (slower than RE2), lambda-wrapped parsing.
- `PROFILE_DIR=dir` writes DuckDB operator timings per statement (times are CPU, summed over threads).
