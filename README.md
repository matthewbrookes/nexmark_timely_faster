# NEXMark benchmark for Timely with FASTER

A lot of this implementation is derived from Moritz Hoffman's implementation in [Megaphone](https://github.com/strymon-system/megaphone)

## Running a Query
Each query can be run for a specified duration (in seconds) and with a given event generation rate
```bash
$ cargo run --release -- --duration 1000 --rate 1000000 --queries q3_faster
```

## Running on multiple workers/processes
Timely Dataflow accepts configuration via arguments supplied at runtime. These can be passed by adding an extra `--` between the line above and Timely's arguments.

For example to run with four workers:
```bash
$ cargo run --release -- --duration 1000 --rate 1000000 --queries q3_faster -- -w 4
```