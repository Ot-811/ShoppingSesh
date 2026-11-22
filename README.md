# ShoppingSesh

C++17, zero dependencies, one static binary. Radix-partitioned parallel pipeline
(parse -> dedup on event_id -> person resolution via union-find -> sort/sessionize ->
sliding-window click credit and assists) with a hand-written Parquet writer
(PLAIN, uncompressed, one file per worker thread).

Correctness checks in `tools/`:
* `reference.py` - slow, literal implementation of rules.md (passes all 36 fixtures)
* `pqread.py` - strict mini Parquet reader that validates the writer's structure
* `gen.py` - dirty-data generator (dups, bad ts forms, quotes, chains, shared devices, leap windows)
* `make fixtures`, `make verify SEED=n`

Note: the run is I/O + parse bound; thread count comes from `NCPU`.
