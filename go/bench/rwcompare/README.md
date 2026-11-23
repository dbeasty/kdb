# rwcompare: KDB vs MongoDB, basic reads and writes

YCSB-shaped cells (1 KB documents of ten 100-byte fields; zipfian point reads, single-field
updates, 50/50 and 95/5 mixes, inserts) at fixed operation counts, against a kdb-service, KDB
embedded in the benchmark process, or a mongod. Its own module, so the MongoDB driver never becomes
a dependency of KDB itself.

```sh
./run.sh kdb   fast    mylabel -docs 100000 -concurrency 1,8,32   # kdb-service built from this checkout
./run.sh mongo fast    mongo   -docs 100000 -concurrency 1,8,32   # mongo:7
./run.sh kdb-embed durable emb -docs 100000 -concurrency 1,8,32
KDB_BIN=/path/to/linux/kdb-service KDB_FLAGS="--sync-mode fast" KDB_ENV="KDB_PREALLOCATE_SEGMENTS=true" \
  ./run.sh kdb durable tuned -cells update,insert
```

`fast` is KDB `--durability async --async-sync-interval-ms 100` against MongoDB `{w:1}`; `durable` is
KDB `--durability sync` against `{j:true}`. Each run uses a fresh volume, gives the database 4 CPUs and
3 GB (`SERVER_CPUS`, `SERVER_MEM`) and runs the client in its own 4-CPU container on the same Docker
network. Results append to `results/<date>/results.jsonl`.

Don't edit `run.sh` while a run uses it, and check the `# load:` line of every run: other jobs on
the machine show up as fake regressions. Method, results and the plan they feed:
`docs/kdb-basic-rw-performance-plan.md`.
