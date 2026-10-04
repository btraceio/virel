// Capture/encoding experiment, not a complete VIREL writer. See README.md.
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <sys/resource.h>

using Clock = std::chrono::steady_clock;
constexpr size_t BUFFER_SIZE = 16384, BUFFER_COUNT = 8, MAX_FRAME = 512;
constexpr size_t SAMPLE_INTERVAL = 127;
using Bytes = std::array<uint8_t, MAX_FRAME>;

uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        Clock::now().time_since_epoch()).count();
}
double cpu_seconds() {
    rusage r{};
    if (getrusage(RUSAGE_SELF, &r)) throw std::runtime_error("getrusage failed");
    return r.ru_utime.tv_sec + r.ru_utime.tv_usec / 1e6 +
           r.ru_stime.tv_sec + r.ru_stime.tv_usec / 1e6;
}
uint64_t zigzag(int64_t v) {
    return (uint64_t(v) << 1) ^ (uint64_t(0) - uint64_t(v < 0));
}
uint8_t* varint(uint8_t* p, uint64_t v) {
    for (int i = 0; i < 8 && v >= 128; ++i) {
        *p++ = uint8_t(v) | 128;
        v >>= 7;
    }
    *p++ = uint8_t(v);
    return p;
}
uint64_t read_varint(const uint8_t*& p, const uint8_t* end) {
    uint64_t value = 0;
    for (int i = 0; i < 9; ++i) {
        if (p == end) throw std::runtime_error("truncated varint");
        uint8_t b = *p++;
        value |= uint64_t(i == 8 ? b : b & 127) << (i * 7);
        if (i == 8 || b < 128) return value;
    }
    throw std::runtime_error("unreachable");
}

struct Config {
    std::string mode = "staged";
    size_t producers = 1, events = 200000, payload = 0, cardinality = 1;
    uint64_t rate = 200000, rotate = 0, stall_ms = 0;
    bool verify = false;
};
struct alignas(64) Buffer {
    std::atomic<bool> ready{false};
    size_t used = 0;
    std::array<uint8_t, BUFFER_SIZE> bytes{};
};
struct Producer {
    std::array<Buffer, BUFFER_COUNT> buffers;
    alignas(64) std::atomic<bool> done{false};
    size_t write_index = 0, cursor = 0;
    uint64_t accepted = 0, dropped = 0, intermediate_bytes = 0;
    std::vector<uint64_t> accepted_latency, dropped_latency;
};
// Internal record prefix. Copy with memcpy: byte-buffer records need not align.
struct Prefix { uint64_t ticks; uint32_t length; uint32_t reserved; };
static_assert(sizeof(Prefix) == 16, "unexpected prefix padding");
struct Raw { uint64_t sequence, producer, value; };
static_assert(sizeof(Raw) == 24, "unexpected raw padding");

uint64_t value_for(uint64_t sequence) {
    return sequence * UINT64_C(11400714819323198485);
}
using Fixture = std::array<uint8_t, 256>;
std::vector<Fixture> fixtures(size_t count) {
    std::vector<Fixture> out(count);
    for (size_t i = 0; i < count; ++i) {
        uint64_t x = i + 1;
        for (auto& b : out[i]) {
            x = x * UINT64_C(6364136223846793005) + 1;
            b = uint8_t('!' + (x >> 32) % 90); // valid single-byte UTF-8
        }
    }
    return out;
}
size_t encode_payload(uint8_t* out, const Raw& r, const uint8_t* text, size_t n) {
    auto* p = varint(out, r.sequence);
    p = varint(p, r.producer);
    p = varint(p, r.value);
    *p++ = 3; // inline UTF-8
    p = varint(p, n);
    std::memcpy(p, text, n);
    return size_t(p + n - out);
}
size_t frame(uint8_t* out, uint64_t ticks, uint64_t anchor,
             const uint8_t* payload, size_t n) {
    // A fixed two-byte padded size prefix includes its own width.
    auto* p = varint(out + 2, 2); // experimental event type
    int64_t offset = ticks >= anchor ? int64_t(ticks - anchor) : -int64_t(anchor - ticks);
    p = varint(p, zigzag(offset));
    std::memcpy(p, payload, n);
    size_t total = size_t(p + n - out);
    if (total >= MAX_FRAME) throw std::runtime_error("frame too large");
    out[0] = uint8_t(total & 127) | 128;
    out[1] = uint8_t(total >> 7);
    return total;
}
bool append(Producer& producer, uint64_t ticks, const uint8_t* body, size_t n) {
    auto* b = &producer.buffers[producer.write_index];
    if (b->ready.load(std::memory_order_acquire)) return false;
    size_t length = sizeof(Prefix) + n;
    if (producer.cursor + length > BUFFER_SIZE) {
        b->used = producer.cursor;
        b->ready.store(true, std::memory_order_release);
        producer.cursor = 0;
        producer.write_index = (producer.write_index + 1) % BUFFER_COUNT;
        b = &producer.buffers[producer.write_index];
        if (b->ready.load(std::memory_order_acquire)) return false;
    }
    Prefix prefix{ticks, uint32_t(n), 0};
    std::memcpy(b->bytes.data() + producer.cursor, &prefix, sizeof(prefix));
    std::memcpy(b->bytes.data() + producer.cursor + sizeof(prefix), body, n);
    producer.cursor += length;
    producer.intermediate_bytes += length;
    return true;
}
void finish(Producer& producer) {
    if (producer.cursor) {
        auto& b = producer.buffers[producer.write_index];
        b.used = producer.cursor;
        b.ready.store(true, std::memory_order_release);
    }
    producer.done.store(true, std::memory_order_release);
}

struct Sink {
    std::array<uint8_t, 65536> bytes{};
    size_t cursor = 0;
    uint64_t count = 0, wire_bytes = 0, digest = 0;
    // Observable bounded memory sink; no filesystem I/O or full-byte hashing.
    void write(const uint8_t* p, size_t n) {
        if (cursor + n > bytes.size()) cursor = 0;
        std::memcpy(bytes.data() + cursor, p, n);
        cursor += n;
        wire_bytes += n;
        ++count;
        digest = digest * 33 + p[0] + p[n - 1];
    }
    uint64_t checksum() const {
        uint64_t h = digest;
        for (auto b : bytes) h = h * 33 + b;
        return h;
    }
};
void verify_frame(const uint8_t* bytes, size_t n, uint64_t ticks, uint64_t anchor,
                  size_t producer, const Config& c, const std::vector<Fixture>& values,
                  uint64_t& previous, bool& seen) {
    auto* p = bytes;
    auto* end = bytes + n;
    if (read_varint(p, end) != n || read_varint(p, end) != 2)
        throw std::runtime_error("invalid framing");
    auto z = read_varint(p, end);
    int64_t offset = int64_t(z >> 1) ^ -int64_t(z & 1);
    if (uint64_t(anchor + uint64_t(offset)) != ticks)
        throw std::runtime_error("timestamp mismatch");
    auto sequence = read_varint(p, end);
    if ((seen && sequence <= previous) || sequence >= c.events)
        throw std::runtime_error("producer order mismatch");
    previous = sequence;
    seen = true;
    if (read_varint(p, end) != producer || read_varint(p, end) != value_for(sequence))
        throw std::runtime_error("scalar mismatch");
    if (p == end || *p++ != 3 || read_varint(p, end) != c.payload ||
        size_t(end - p) != c.payload ||
        std::memcmp(p, values[sequence % c.cardinality].data(), c.payload))
        throw std::runtime_error("payload mismatch");
}
uint64_t percentile(std::vector<uint64_t>& values, double fraction) {
    if (values.empty()) return 0;
    std::sort(values.begin(), values.end());
    return values[size_t(fraction * double(values.size() - 1))];
}
void encoding_selftest() {
    for (uint64_t value : {UINT64_C(0), UINT64_C(127), UINT64_C(128),
                          UINT64_C(1) << 56, UINT64_MAX}) {
        Bytes b{};
        auto* end = varint(b.data(), value);
        const uint8_t* p = b.data();
        if (read_varint(p, end) != value || p != end || end - b.data() > 9)
            throw std::runtime_error("varint selftest failed");
    }
    for (int64_t value : {INT64_MIN, INT64_C(-129), INT64_C(-1), INT64_C(0),
                         INT64_C(127), INT64_MAX}) {
        auto z = zigzag(value);
        if ((int64_t(z >> 1) ^ -int64_t(z & 1)) != value)
            throw std::runtime_error("zigzag selftest failed");
    }
    Bytes b{}, payload{};
    auto n = frame(b.data(), 10, 100, payload.data(), 0);
    const uint8_t* p = b.data();
    read_varint(p, b.data() + n);
    read_varint(p, b.data() + n);
    if (read_varint(p, b.data() + n) != zigzag(-90))
        throw std::runtime_error("pre-anchor selftest failed");
}

int main(int argc, char** argv) try {
    Config c;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--verify") { c.verify = true; continue; }
        if (i + 1 == argc) throw std::runtime_error("missing option value");
        std::string value = argv[++i];
        if (arg == "--mode") c.mode = value;
        else {
            if (value.empty() || value[0] == '-') throw std::runtime_error("invalid number");
            size_t end = 0;
            auto n = std::stoull(value, &end);
            if (end != value.size()) throw std::runtime_error("invalid number");
            if (arg == "--producers") c.producers = n;
            else if (arg == "--events") c.events = n;
            else if (arg == "--payload") c.payload = n;
            else if (arg == "--cardinality") c.cardinality = n;
            else if (arg == "--rate") c.rate = n;
            else if (arg == "--rotate") c.rotate = n;
            else if (arg == "--stall-ms") c.stall_ms = n;
            else throw std::runtime_error("unknown option: " + arg);
        }
    }
    if ((c.mode != "staged" && c.mode != "payload" && c.mode != "direct") ||
        !c.events || c.events > 100000000 || !c.producers || c.producers > 64 ||
        c.payload > 256 || !c.cardinality || c.cardinality > 65536 ||
        c.rate > 1000000000 || c.stall_ms > 10000 ||
        (c.mode == "direct" && c.rotate))
        throw std::runtime_error("invalid configuration (direct requires rotate=0)");
    encoding_selftest();
    auto values = fixtures(c.cardinality);
    std::vector<std::unique_ptr<Producer>> producers;
    for (size_t i = 0; i < c.producers; ++i) {
        auto p = std::make_unique<Producer>();
        p->accepted_latency.reserve(c.events / SAMPLE_INTERVAL + 1);
        p->dropped_latency.reserve(c.events / SAMPLE_INTERVAL + 1);
        producers.push_back(std::move(p));
    }
    std::atomic<bool> go{false};
    std::atomic<size_t> ready{0};
    uint64_t start = 0;
    Sink sink;
    std::vector<std::thread> workers;
    for (size_t id = 0; id < c.producers; ++id) {
        workers.emplace_back([&, id] {
            auto& producer = *producers[id];
            Bytes body{}, payload{};
            ready.fetch_add(1);
            while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
            for (size_t i = 0; i < c.events; ++i) {
                // Pace bursts of 64 outside the recording call. Never retry drops.
                if (c.rate && i % 64 == 0) {
                    auto target = start + uint64_t(double(i) * 1e9 / double(c.rate));
                    std::this_thread::sleep_until(Clock::time_point(std::chrono::nanoseconds(target)));
                }
                bool sample = (i + id * 17) % SAMPLE_INTERVAL == 0;
                uint64_t begin = sample ? now_ns() : 0;
                auto ticks = now_ns();
                Raw r{uint64_t(i), uint64_t(id), value_for(i)};
                const auto* text = values[i % c.cardinality].data();
                size_t n;
                if (c.mode == "staged") {
                    std::memcpy(body.data(), &r, sizeof(r));
                    std::memcpy(body.data() + sizeof(r), text, c.payload);
                    n = sizeof(r) + c.payload;
                } else {
                    n = encode_payload(payload.data(), r, text, c.payload);
                    if (c.mode == "direct") n = frame(body.data(), ticks, start, payload.data(), n);
                    else std::memcpy(body.data(), payload.data(), n);
                }
                bool accepted = append(producer, ticks, body.data(), n);
                uint64_t elapsed = sample ? now_ns() - begin : 0;
                if (accepted) ++producer.accepted; else ++producer.dropped;
                if (sample) (accepted ? producer.accepted_latency : producer.dropped_latency).push_back(elapsed);
            }
            finish(producer);
        });
    }
    std::exception_ptr consumer_error;
    std::thread consumer([&] {
        std::vector<size_t> indices(c.producers, 0);
        std::vector<uint64_t> previous(c.producers, 0);
        std::unique_ptr<bool[]> seen(new bool[c.producers]{});
        Bytes payload{}, output{};
        ready.fetch_add(1);
        while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
        try {
            uint64_t anchor = start;
            if (c.stall_ms) std::this_thread::sleep_for(std::chrono::milliseconds(c.stall_ms));
            for (;;) {
                bool all_done = true, progress = false;
                for (size_t id = 0; id < c.producers; ++id) {
                    auto& producer = *producers[id];
                    bool done = producer.done.load(std::memory_order_acquire);
                    auto& b = producer.buffers[indices[id]];
                    if (!b.ready.load(std::memory_order_acquire)) {
                        if (!done) all_done = false;
                        continue;
                    }
                    all_done = false;
                    progress = true;
                    for (size_t offset = 0; offset < b.used;) {
                        Prefix prefix{};
                        std::memcpy(&prefix, b.bytes.data() + offset, sizeof(prefix));
                        offset += sizeof(prefix);
                        const auto* input = b.bytes.data() + offset;
                        if (prefix.length > MAX_FRAME || offset + prefix.length > b.used)
                            throw std::runtime_error("invalid staging record");
                        if (c.rotate && sink.count && sink.count % c.rotate == 0)
                            anchor = now_ns(); // models rebasing only, not full chunk rotation
                        const uint8_t* wire;
                        size_t n;
                        if (c.mode == "direct") { wire = input; n = prefix.length; }
                        else {
                            const uint8_t* encoded = input;
                            size_t length = prefix.length;
                            if (c.mode == "staged") {
                                Raw raw{};
                                std::memcpy(&raw, input, sizeof(raw));
                                length = encode_payload(payload.data(), raw, input + sizeof(raw), c.payload);
                                encoded = payload.data();
                            }
                            n = frame(output.data(), prefix.ticks, anchor, encoded, length);
                            wire = output.data();
                        }
                        if (c.verify) verify_frame(wire, n, prefix.ticks, anchor, id, c, values,
                                                   previous[id], seen[id]);
                        sink.write(wire, n);
                        offset += prefix.length;
                    }
                    b.ready.store(false, std::memory_order_release);
                    indices[id] = (indices[id] + 1) % BUFFER_COUNT;
                }
                if (all_done) break;
                if (!progress) std::this_thread::sleep_for(std::chrono::microseconds(20));
            }
        } catch (...) { consumer_error = std::current_exception(); }
    });
    while (ready.load() != c.producers + 1) std::this_thread::yield();
    auto cpu_start = cpu_seconds();
    start = now_ns();
    go.store(true, std::memory_order_release);
    for (auto& t : workers) t.join();
    consumer.join();
    double elapsed = double(now_ns() - start) / 1e9;
    double cpu = cpu_seconds() - cpu_start;
    if (consumer_error) std::rethrow_exception(consumer_error);
    std::vector<uint64_t> accepted_latency, dropped_latency;
    uint64_t accepted = 0, dropped = 0, intermediate = 0;
    for (const auto& p : producers) {
        accepted += p->accepted; dropped += p->dropped; intermediate += p->intermediate_bytes;
        accepted_latency.insert(accepted_latency.end(), p->accepted_latency.begin(), p->accepted_latency.end());
        dropped_latency.insert(dropped_latency.end(), p->dropped_latency.begin(), p->dropped_latency.end());
    }
    if (sink.count != accepted || accepted + dropped != c.events * c.producers)
        throw std::runtime_error("event accounting mismatch");
    std::cout << "{\"mode\":\"" << c.mode << "\",\"producers\":" << c.producers
        << ",\"events_per_producer\":" << c.events << ",\"rate_per_producer\":" << c.rate
        << ",\"payload_bytes\":" << c.payload << ",\"cardinality\":" << c.cardinality
        << ",\"rotate_every\":" << c.rotate << ",\"stall_ms\":" << c.stall_ms
        << ",\"verification\":" << (c.verify ? "true" : "false")
        << ",\"accepted\":" << accepted << ",\"dropped\":" << dropped
        << ",\"wall_seconds\":" << elapsed << ",\"cpu_seconds\":" << cpu
        << ",\"cpu_ns_per_attempt\":" << cpu * 1e9 / double(accepted + dropped)
        << ",\"cpu_ns_per_accepted\":" << (accepted ? cpu * 1e9 / double(accepted) : 0)
        << ",\"accepted_per_second\":" << double(accepted) / elapsed
        << ",\"cpu_cores\":" << cpu / elapsed
        << ",\"wire_bytes_per_accepted\":" << (accepted ? double(sink.wire_bytes) / accepted : 0)
        << ",\"intermediate_bytes_per_accepted\":" << (accepted ? double(intermediate) / accepted : 0)
        << ",\"accepted_samples\":" << accepted_latency.size()
        << ",\"producer_p50_ns\":" << percentile(accepted_latency, .5)
        << ",\"producer_p99_ns\":" << percentile(accepted_latency, .99)
        << ",\"producer_p999_ns\":" << percentile(accepted_latency, .999)
        << ",\"producer_max_sampled_ns\":" << percentile(accepted_latency, 1)
        << ",\"dropped_samples\":" << dropped_latency.size()
        << ",\"drop_p99_ns\":" << percentile(dropped_latency, .99)
        << ",\"checksum\":" << sink.checksum() << "}\n";
} catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
}
