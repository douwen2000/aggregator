# Market-Data Aggregator

A C++17 system that aggregates BTCUSDT order-book market data from three
exchanges into a single consolidated book and exposes it over a gRPC streaming
API. Several client services subscribe to the aggregator and publish derived
views to stdout.

## Services

| Service                   | Subscribes with | Role                                                      |
|---------------------------|-----------------|-----------------------------------------------------------|
| `aggregator_server`       | —               | Connects to 3 venues, consolidates books, serves gRPC      |
| `bbo_publisher`           | `BBO`           | prints the consolidated Best Bid/Offer                     |
| `volume_bands_publisher`  | `VOLUME_BANDS`  | prints Volume Bands (1M/5M/10M/25M/50M+)                   |
| `price_bands_publisher`   | `PRICE_BANDS`   | prints Price Bands (BBO ± 50/100/200/500/1000 bps)         |

## Architecture

```
           ┌────────────┐  ┌────────────┐  ┌────────────┐
  feeds →  │  binance   │  │    okx     │  │   bybit    │   (ExchangeFeed)
           └─────┬──────┘  └─────┬──────┘  └─────┬──────┘
                 │ publish(): claim a slot, fill it IN PLACE, publish
                 └───────────────┬──────────────────┘
                                 ▼
              ┌─────────────────────────────────────┐
              │  VenueRing — Disruptor, 1024 slots  │  lock-free MPSC
              │  pre-allocated, no queue node alloc │
              └──────────────────┬──────────────────┘
                                 │ waitFor() → one batch of sequences
                                 ▼
                     ┌───────────────────────────┐
                     │   AggregatorEngine        │  THE single consumer
                     │   batch drain + throttle  │
                     └─────────────┬─────────────┘
                                   ▼
                     ┌───────────────────────────┐
                     │   ConsolidatedBook        │  no locks at all:
                     │   (sizes summed/price)    │  owned exclusively
                     └─────────────┬─────────────┘  by the consumer
                                   │ BookUpdatePtr (shared_ptr, shared)
                                   ▼
                     ┌───────────────────────────┐
                     │  AggregatorServiceImpl    │  fan out, then narrow
                     │                           │  per subscriber's mode
                     └─────────────┬─────────────┘
           ┌───────────────────────┼───────────────────────┐
           ▼                       ▼                       ▼
     bbo_publisher      volume_bands_publisher      price_bands_publisher
           └───────────────────────┼───────────────────────┘
                                   ▼
                                stdout
```

Data model: every venue emits a **full top-N depth snapshot** (N=50) on a fixed
cadence. The producer claims a ring slot and fills the event **in place**, so no
intermediate snapshot object is ever built. The engine owns the
`ConsolidatedBook` exclusively, merges venues (summing sizes at identical
prices), and publishes at most one update per 10 ms (≤100 Hz) — each wake-up
drains every available event as a single batch, so bursty inbound traffic does
not become bursty outbound traffic. The published `BookUpdate` carries the
consolidated top-50 plus a **trimmed** per-venue view (10 levels) and is handed
to all subscribers as a `shared_ptr`, so fan-out copies a pointer, not a book.

## Disruptor pipeline

Inbound market data goes through a single lock-free ring (`common/disruptor.h`)
instead of a mutex-protected queue:

```
producer threads (3)                       consumer thread (1)
  claim()  ── CAS on claim_seq_              waitFor(next) ── batch of seqs
  slot(s)  ── fill IN PLACE, no copy         apply to ConsolidatedBook
  publish(s) ── store the slot's lap         publish ≤100 Hz
```

| Property | How |
|---|---|
| Multi-producer claim | CAS on `claim_seq_`; publishing is a store to the slot's lap counter. No mutex on the producer path. |
| Zero allocation | 1024 slots pre-allocated (~2 MB). The producer writes into the slot through a callback, so no queue node and no intermediate snapshot object is ever built. |
| No locks on the book | `ConsolidatedBook` is owned **exclusively** by the engine thread (single-writer principle), so it needs no mutex at all. This is the main correctness win, not just a speed win. |
| Batch drain | `waitFor(seq)` returns the highest *contiguously published* sequence, so one wake-up drains the whole backlog and emits a single update. Bursty inbound traffic does not become bursty outbound traffic. |
| Cache-line padding | `Sequence` is `alignas(64)` and padded to 64 bytes (`static_assert(sizeof == 64)`), so producers and the consumer do not false-share. |
| Backpressure | When the ring is full, producers wait on the consumer cursor per the configured `WaitStrategy` (`BusySpin` / `Yielding` / `Sleeping`) rather than growing an unbounded queue. |

**Why slots are marked with a lap, not a sequence number.** Slot *i* serves
sequences `i`, `i + 1024`, `i + 2048`, … If the slot stored the sequence number,
a stale value left over from a previous lap could "match" a later sequence and
the consumer would accept a slot that was never published — silently reading
garbage. Each slot instead stores a **lap** (`seq >> log2(capacity)`), which is
unique per lap: slot *i* holds lap *L* **iff** sequence `L*1024 + i` has been
published.

> **Gotcha:** `waitFor(seq, keepRunning)` takes a **run** flag — `true` means keep
> going, and it returns `-1` once the flag clears. Passing a "stop" flag instead
> inverts the polarity and the consumer exits after its very first batch. That
> variant of the bug once left the whole system alive but stalled after 6 events;
> `test_ring.cpp` now locks the polarity down.

## Subscription modes

`SubscribeRequest.mode` selects what the server pushes. **The server computes the
bands**, so a subscriber receives only the payload it asked for — less bandwidth,
and no duplicated math in every client.

| `mode` | Payload | Meaning |
|---|---|---|
| `BBO` | `BboUpdate` | best bid/offer with sizes |
| `VOLUME_BANDS` | `VolumeBandsUpdate` | price reached at 1M/5M/10M/25M/50M+ USD |
| `PRICE_BANDS` | `PriceBandsUpdate` | price + cumulative size at 50/100/200/500/1000 bps |
| `FULL_BOOK` | `BookUpdate` | whole consolidated book + per-venue detail (debugging) |
| `MODE_UNSPECIFIED` | as `FULL_BOOK` | default when a client omits `mode` |

### `Side` names the order book, not the trade direction

This is the one thing that trips people up, so it is stated here and locked by
`band_side_names_the_order_book` in `test_publishers.cpp`:

* **`buy` = 买盘 / bid book** — bands walk **down** from `best_bid`
* **`sell` = 卖盘 / ask book** — bands walk **up** from `best_ask`

Both modes use this same convention.

### Mode 1 — BBO

```
[BBO] 2026-10-26 01:53:53.981 | seq=315 | bid=60001.88 (41.0000) | ask=60007.40 (52.0000) | mid=60004.64 | spread=5.53 (0.92 bps)
```

### Mode 2 — Volume Bands

`1M` is the price reached once 1M USD of depth has been consumed from the top of
book. Prices descend on the bid book and ascend on the ask book; if the book is
shallower than the target, the deepest available price is returned.

```
[VolumeBands] 2026-10-26 01:53:58.449 | Side: BUY  | 1M: 59950.50 5M: 59949.90 10M: 59948.71 25M: 59946.46 50M+: 59944.38
[VolumeBands] 2026-10-26 01:53:58.449 | Side: SELL | 1M: 59956.49 5M: 59957.09 10M: 59958.29 25M: 59960.63 50M+: 59963.03
```

### Mode 3 — Price Bands

1 bp = 1e-4, so a band's boundary price is `BBO * (1 ± bps/1e4)`:

```
sell:  price = best_ask * (1 + bps/1e4)
buy:   price = best_bid * (1 - bps/1e4)
```

`Vol` is the **cumulative** size in **base units** (BTC) resting between the BBO
and that band, so it grows monotonically as the band widens.

```
[PriceBands] 2026-10-26 01:54:03.504 | Side: BUY  | +50bps: 59628.91 (Vol: 2048.14) +100bps: 59329.27 (Vol: 2048.14) +200bps: 58729.98 (Vol: 2048.14) +500bps: 56932.12 (Vol: 2048.14) +1000bps: 53935.70 (Vol: 2048.14)
[PriceBands] 2026-10-26 01:54:03.504 | Side: SELL | +50bps: 60236.03 (Vol: 2020.49) +100bps: 60535.72 (Vol: 2020.49) +200bps: 61135.08 (Vol: 2020.49) +500bps: 62933.17 (Vol: 2020.49) +1000bps: 65929.99 (Vol: 2020.49)
```

> With the simulated feeds the book spans only ~12 USD, so even the narrowest
> band (+50 bps ≈ 300 USD) already covers the whole book and all five `Vol`
> values come out equal. That is a property of the simulated data, not of the
> band math — `band_volume_monotonic_in_bps` asserts monotonicity, and the
> numbers separate once the book is deep enough.

### Where the narrowing happens

The queue still carries one shared `BookUpdate`. **Each subscriber's own RPC
thread** narrows it to its requested mode before writing, which means:

* the engine and the fan-out path are untouched by mode handling;
* band math runs per subscriber with no shared state, so subscribers of
  different modes never contend with each other;
* only the requested payload goes on the wire.

## Build

### Docker (recommended)

```bash
docker compose up --build
```

This builds a single image containing all four binaries and launches the
aggregator plus the three publishers. Aggregator listens on `:50051`; publishers
connect to `aggregator:50051`.

### Local (requires gRPC + Protobuf)

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
# run
./build/aggregator_server --feeds sim &
./build/bbo_publisher --target localhost:50051
./build/volume_bands_publisher --target localhost:50051
./build/price_bands_publisher --target localhost:50051
```

### Tests

The unit tests (30 suites) exercise the pure aggregation/consolidation/band math
and the ring, and do **not** require gRPC:

```bash
cd build && ctest --output-on-failure     # or: ./run_tests
```

`src/tests/test_ring.cpp` spawns **real producer/consumer threads** and covers the
parts that fail silently rather than loudly — multi-producer loss, reordering and
slot aliasing, plus the `waitFor` run-flag polarity that once stalled the whole
system. `src/tests/test_market_state.cpp` locks the shared-mid invariant that
keeps the consolidated book uncrossed.

## gRPC API

`Subscribe` returns a stream of `MarketUpdate`, a `oneof` whose single set field
matches the requested mode (see [Subscription modes](#subscription-modes)).

```proto
service Aggregator {
  rpc Subscribe(SubscribeRequest) returns (stream MarketUpdate);
}

message SubscribeRequest {
  string        symbol = 1; // empty => default symbol (BTCUSDT)
  SubscribeMode mode   = 2;
}

enum SubscribeMode {
  MODE_UNSPECIFIED = 0; // treated as FULL_BOOK
  BBO              = 1;
  VOLUME_BANDS     = 2;
  PRICE_BANDS      = 3;
  FULL_BOOK        = 4;
}

message MarketUpdate {
  oneof payload {
    BboUpdate         bbo          = 1;
    VolumeBandsUpdate volume_bands = 2;
    PriceBandsUpdate  price_bands  = 3;
    BookUpdate        book         = 4;
  }
}

message BookUpdate {          // FULL_BOOK only
  string symbol       = 1;
  uint64 sequence     = 2;
  int64  timestamp_us = 3;
  repeated Level bids   = 4;  // consolidated top-50, descending price
  repeated Level asks   = 5;  // consolidated top-50, ascending price
  repeated VenueBook venues = 6; // per-venue, trimmed to 10 levels
}
```

## Technical decisions & extensibility

* **Exchange abstraction.** All feeds implement `ExchangeFeed`; the aggregator
  depends only on that interface. Swapping the simulator for a live exchange is a
  localized change (see "Real CEX connectors" below).
* **Snapshot model.** Exchanges typically expose either snapshot streams or
  snapshot+delta streams. Consuming full snapshots keeps reconciliation trivially
  correct; the aggregator is written to accept snapshots from every venue.
* **Consolidation = sum-by-price.** At each price level the sizes from all venues
  are added, so the consolidated book reflects total resting liquidity.
* **Throttled fan-out.** Outbound publication is rate-limited (≤100 Hz, one
  update per 10 ms) independently of inbound feed rate, and each update is a
  batch drain of everything currently queued, bounding CPU and network under load.
* **Locking discipline.** `ConsolidatedBook` needs **no mutex at all**: the engine
  is the ring's single consumer and owns the book exclusively (single-writer
  principle). The only locks left are in the ring's shutdown path and the
  subscriber registry, both off the hot path.
* **Band math lives in one place.** `common/bands.h` holds the volume/price band
  math and is shared by the service (which computes it server-side) and the
  tests (which lock the definitions and the `buy`/`sell` book mapping).
* **Per-subscriber narrowing.** Mode conversion happens in each subscriber's own
  RPC thread, not in the engine, so adding a mode never touches the hot path.

## Scalability

* The consolidated book is O(venues × depth) per update. Fan-out is O(1) to
  *hand off* (one `shared_ptr` copy per subscriber, no book copy) but still
  O(subscribers) to serialise and write, and mode narrowing is O(1) — BBO — or
  O(bands) for the band modes, per subscriber, on its own thread.
* **Multiple symbols:** the subscriber registry is already keyed by symbol
  (`subs_[symbol]`), so the remaining work is one `ConsolidatedBook` + engine per
  symbol (the book is currently owned by a single consumer per symbol).
* The gRPC server is synchronous-per-stream; for very high subscriber counts one
  would move to the async gRPC API (callbacks / completion queue) — the service
  boundary (engine `UpdateSink` → `service.onUpdate` → per-subscriber queue) is
  already isolated to make that swap easy.

## Real CEX connectors (extension)

The default build uses deterministic, offline **simulated** feeds named
`binance`/`okx`/`bybit` so the whole system runs and is testable without internet.
A reference real-connector implementation using Boost.Beast (TLS WebSocket) is
provided in `src/feed/real_exchanges.cpp` and compiled when built with:

```bash
cmake -B build -DAGG_REAL_FEEDS=ON ..   # needs Boost.Beast + Boost.Json + OpenSSL
./build/aggregator_server --feeds real
```

It connects to Binance (`<symbol>@depth20@100ms`), OKX (`books50`) and Bybit
(`orderbook.50`) public depth streams and feeds the same `ConsolidatedBook`.
This is off by default so the container build has no external dependency.
