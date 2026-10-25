// Regression tests for the Disruptor ring buffer.
//
// This component has failed silently twice (both times as "the server is up but
// no events flow"), so it gets a real concurrency test: multiple producers, one
// consumer, and a tiny capacity to force constant wrap-around.
#include "test/test_harness.h"
#include "common/disruptor.h"

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

using namespace aggregator;

TEST(ring_sequence_padding) {
    // The whole point of the padding: a cursor must not share a cache line.
    test::check(sizeof(Sequence) == 64, "Sequence occupies exactly one cache line");
    test::check(alignof(Sequence) == 64, "Sequence is cache-line aligned");
}

TEST(ring_single_producer_ordering) {
    // 20 events into a 64-slot ring: no wrap here, but it exercises the
    // batch-return path. (Claiming more than Capacity with no consumer running
    // would block in claim() by design.)
    RingBuffer<uint64_t, 64> ring(WaitStrategy::BusySpin);

    for (uint64_t i = 0; i < 20; ++i) {
        const int64_t s = ring.claim();
        ring.slot(s)    = i;
        ring.publish(s);
    }

    std::atomic<bool> running{true};
    const int64_t hi = ring.waitFor(0, &running);
    test::check(hi == 19, "consumer sees every published sequence in one batch");
    test::check(ring.slot(19) == 19, "last written slot is intact");
    ring.publishConsumed(hi);
}

TEST(ring_multi_producer_no_loss) {
    constexpr size_t kCap = 64;
    constexpr int    kProducers = 3;
    constexpr int    kEach = 2000;

    RingBuffer<uint64_t, kCap> ring(WaitStrategy::Yielding);

    std::atomic<bool> running{true};
    std::atomic<long> received{0};
    std::atomic<long> bad{0};
    std::vector<uint64_t> last(kProducers, 0);

    std::thread consumer([&] {
        int64_t next = 0;
        for (;;) {
            const int64_t hi = ring.waitFor(next, &running);
            if (hi < 0) break;
            for (int64_t s = next; s <= hi; ++s) {
                const uint64_t v   = ring.slot(s);
                const uint64_t pid = v >> 32;
                const uint64_t cnt = v & 0xffffffffu;
                if (pid >= kProducers) { ++bad; continue; }  // overwritten slot
                if (cnt != last[pid] + 1) ++bad;             // lost/dup/reordered
                last[pid] = cnt;
                ++received;
            }
            ring.publishConsumed(hi);
            next = hi + 1;
            if (received >= (long)(kProducers * kEach)) break;
        }
    });

    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p] {
            for (int i = 1; i <= kEach; ++i) {
                const int64_t s = ring.claim();
                ring.slot(s)    = ((uint64_t)p << 32) | (uint64_t)i;
                ring.publish(s);
            }
        });
    }
    for (auto& t : producers) t.join();

    // Producers are done; give the consumer time to drain, then shut it down.
    while (received < (long)(kProducers * kEach)) std::this_thread::yield();
    running = false;
    consumer.join();

    test::check(received == kProducers * kEach,
                "no events lost under wrap-around");
    test::check(bad == 0, "no duplicate, reordered or overwritten events");
}

TEST(ring_waitfor_run_flag_polarity) {
    // Regression: waitFor() used to treat the flag as "stop requested" while
    // callers pass their "keep running" flag, so the consumer quit after its
    // very first batch and the pipeline silently stalled.
    RingBuffer<uint64_t, 8> ring(WaitStrategy::BusySpin);

    std::atomic<bool> keep_going{true};
    const int64_t s = ring.claim();
    ring.slot(s)    = 7;
    ring.publish(s);

    test::check(ring.waitFor(0, &keep_going) == 0,
                "a true run flag means keep going, not stop");

    // With the flag cleared, even an unavailable sequence must return -1.
    std::atomic<bool> stopped{false};
    test::check(ring.waitFor(5, &stopped) == -1,
                "a cleared run flag reports shutdown even when idle");
}

TEST(ring_slot_isolation) {
    // Claimed slots must not alias each other. This guards the lap-marking
    // scheme: a stale lap on slot i would make it look already-published and
    // hand the same slot to two producers, silently corrupting an event.
    RingBuffer<double, 16> ring(WaitStrategy::BusySpin);

    const int64_t s0 = ring.claim();
    ring.slot(s0)    = 1.5;
    const int64_t s1 = ring.claim();
    ring.slot(s1)    = 2.5;
    const int64_t s2 = ring.claim();
    ring.slot(s2)    = 3.5;
    ring.publish(s0);
    ring.publish(s1);
    ring.publish(s2);

    test::check(s0 == 0 && s1 == 1 && s2 == 2, "claims hand out consecutive sequences");
    test::check(ring.slot(s0) == 1.5 && ring.slot(s1) == 2.5 && ring.slot(s2) == 3.5,
                "distinct slots hold distinct values");
}

TEST(ring_wraparound_frees_slots) {
    // Backpressure is by design: claiming past Capacity with no consumer would
    // block forever. With a consumer releasing slots, the producer must be able
    // to wrap around more than once -- i.e. publishConsumed really does free
    // slots, and a recycled slot still carries its own lap.
    constexpr size_t kCap   = 8;
    constexpr int    kTotal = 16;  // 2x capacity -> wraps twice
    RingBuffer<uint64_t, kCap> ring(WaitStrategy::Yielding);

    std::atomic<bool> running{true};
    std::atomic<int>  consumed{0};
    std::atomic<int>  bad{0};

    std::thread consumer([&] {
        int64_t next = 0;
        while (consumed < kTotal) {
            const int64_t hi = ring.waitFor(next, &running);
            if (hi < 0) break;
            for (int64_t s = next; s <= hi; ++s) {
                if (ring.slot(s) != static_cast<uint64_t>(s)) ++bad;
                ++consumed;
            }
            ring.publishConsumed(hi);
            next = hi + 1;
        }
    });

    for (uint64_t i = 0; i < kTotal; ++i) {
        const int64_t s = ring.claim();
        ring.slot(s)    = i;
        ring.publish(s);
    }
    while (consumed < kTotal) std::this_thread::yield();
    running = false;
    consumer.join();

    test::check(consumed == kTotal, "the producer wrapped past capacity twice");
    test::check(bad == 0, "every recycled slot still held its own value");
}
