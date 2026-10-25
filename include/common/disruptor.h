#pragma once

// LMAX Disruptor: multi-producer / single-consumer ring buffer.
//
//   producers:  CAS-claim a slot, fill it in place, publish it
//   consumer:   waits for a slot, then drains EVERYTHING available as one batch
//   slots:      pre-allocated once => zero allocation on the hot path
//
// The real win is the single-consumer discipline: because exactly one thread
// touches the aggregated book, it needs no synchronisation at all. The ring
// only has to move slot ownership between producers and that one consumer.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <utility>

namespace aggregator {

// A sequence counter alone on its cache line. Padding is deliberate: producers
// hammer the claim cursor while the consumer hammers its own cursor, and
// sharing a line would make every update ping-pong between cores.
class alignas(64) Sequence {
public:
    explicit Sequence(int64_t initial = -1) : value_(initial) {}
    Sequence(const Sequence&)            = delete;
    Sequence& operator=(const Sequence&) = delete;

    int64_t get() const { return value_.load(std::memory_order_acquire); }
    void    set(int64_t v) { value_.store(v, std::memory_order_release); }

    bool compareAndSet(int64_t expected, int64_t desired) {
        return value_.compare_exchange_strong(expected, desired,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire);
    }

private:
    std::atomic<int64_t> value_;
    [[maybe_unused]] char pad_[64 - sizeof(std::atomic<int64_t>)];
};

static_assert(sizeof(Sequence) == 64, "Sequence must occupy exactly one cache line");

// floor(log2(n)); Capacity is a power of two, so this is exact.
constexpr size_t log2_floor(size_t n) { return n <= 1 ? 0 : 1 + log2_floor(n >> 1); }

// How a blocked party waits. BusySpin burns a core for the lowest latency;
// Yielding is the sane default; Sleeping trades latency for idle CPU.
enum class WaitStrategy { BusySpin, Yielding, Sleeping };

inline void wait_tick(WaitStrategy s, unsigned& spins) {
    switch (s) {
        case WaitStrategy::BusySpin:
            break;
        case WaitStrategy::Yielding:
            std::this_thread::yield();
            break;
        case WaitStrategy::Sleeping:
            if (spins < 200) {
                ++spins;
            } else if (spins < 400) {
                ++spins;
                std::this_thread::yield();
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
            break;
    }
}

// Ring buffer with multiple producers and a single consumer.
// 'T' must be default-constructible; slots are allocated once, up front.
template <typename T, size_t Capacity>
class RingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");

public:
    static constexpr size_t kCapacity = Capacity;
    static constexpr size_t kMask     = Capacity - 1;
    // Shifting a sequence right by this yields its lap (wrap count).
    static constexpr size_t kShift    = log2_floor(Capacity);

    explicit RingBuffer(WaitStrategy ws = WaitStrategy::Yielding) : ws_(ws) {}

    // ---- producer side (safe for concurrent producers) ----

    // Claim the next slot. Blocks only while the ring is full.
    int64_t claim() {
        unsigned spins = 0;
        for (;;) {
            const int64_t cur  = claim_seq_.get();
            const int64_t next = cur + 1;
            // The slot 'next' maps to must have been consumed on the previous
            // lap before we may overwrite it.
            if (next - static_cast<int64_t>(Capacity) > consumer_seq_.get()) {
                wait_tick(ws_, spins);
                continue;
            }
            if (claim_seq_.compareAndSet(cur, next)) return next;
        }
    }

    T&       slot(int64_t seq)       { return buffer_[seq & kMask]; }
    const T& slot(int64_t seq) const { return buffer_[seq & kMask]; }

    // Make a claimed slot visible to the consumer.
    //
    // Stores the sequence's LAP, not the sequence itself. Storing the sequence
    // would let a leftover value in a reused slot alias onto a later sequence
    // (slot i serves i, i+Capacity, i+2*Capacity, ...), so a consumer could
    // accept a slot that was never published. Laps are distinct per visit, so
    // slot i holds L exactly when sequence L*Capacity + i was published.
    void publish(int64_t seq) { available_[seq & kMask].set(seq >> kShift); }

    // Claim, fill, publish. Serialises nothing: producers fill distinct slots.
    template <typename FillFn>
    void produce(FillFn&& fill) {
        const int64_t seq = claim();
        fill(buffer_[seq & kMask]);
        publish(seq);
    }

    // ---- consumer side (single consumer) ----

    // Block until 'seq' is published, then return the highest contiguously
    // published sequence (>= seq) so the caller can process a whole batch.
    //
    // 'keepRunning' (when non-null) is the caller's run flag: TRUE means keep
    // going. Returns -1 once it goes false, i.e. the caller should shut down.
    // (Passing a "stop" flag here would invert the polarity and make the
    // consumer quit on its very first batch.)
    int64_t waitFor(int64_t seq, const std::atomic<bool>* keepRunning = nullptr) {
        unsigned spins = 0;
        while (available_[seq & kMask].get() != (seq >> kShift)) {
            if (keepRunning && !keepRunning->load(std::memory_order_acquire)) return -1;
            wait_tick(ws_, spins);
        }
        // Extend over everything already published so the caller gets one batch.
        // Bounded by the highest claimed sequence: nothing beyond it can be
        // published yet, which keeps this scan finite and short.
        int64_t hi      = seq;
        const int64_t claimed = claim_seq_.get();
        while (hi < claimed &&
               available_[(hi + 1) & kMask].get() == ((hi + 1) >> kShift)) {
            ++hi;
        }
        return hi;
    }

    // Release every slot up to 'seq' back to the producers.
    void publishConsumed(int64_t seq) { consumer_seq_.set(seq); }

    // The sequence the consumer has released up to (diagnostics).
    int64_t consumerCursor() const { return consumer_seq_.get(); }

private:
    T        buffer_[Capacity];
    // available_[i] holds the lap of the sequence last published into slot i
    // (-1 until first used).
    Sequence available_[Capacity];

    Sequence claim_seq_{-1};     // highest sequence handed to a producer
    Sequence consumer_seq_{-1};  // highest sequence the consumer released

    WaitStrategy ws_;
};

}  // namespace aggregator
