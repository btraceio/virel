// Bounded generation/pool prototype with native draft chunk output.
// See generation-writer.md for remaining live-reader and durability limits.
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <pthread.h>
#include <signal.h>
#include <sys/resource.h>

thread_local bool recording_call = false, measured_worker = false;
std::atomic<uint64_t> recording_allocations{0}, worker_allocations{0};
void allocation_observed() {
    if (recording_call) recording_allocations.fetch_add(1, std::memory_order_relaxed);
    if (measured_worker) worker_allocations.fetch_add(1, std::memory_order_relaxed);
}
void* operator new(std::size_t n) {
    allocation_observed();
    if (auto* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void* operator new[](std::size_t n) { return ::operator new(n); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void* operator new(std::size_t n, std::align_val_t a) {
    allocation_observed(); void* p = nullptr;
    if (posix_memalign(&p, size_t(a), n ? n : 1)) throw std::bad_alloc();
    return p;
}
void* operator new[](std::size_t n, std::align_val_t a) { return ::operator new(n, a); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }

using Clock = std::chrono::steady_clock;
constexpr size_t BUFFER_BYTES = 8192, BUFFER_COUNT = 8, POOL_CAP = 256;
constexpr size_t TABLE_CAP = 512, PROBE_LIMIT = 16, VALUE_BYTES = 64;
constexpr size_t CAPTURE_SAMPLES = BUFFER_BYTES/12+1;
constexpr uint64_t NO_GENERATION = UINT64_MAX;
constexpr uint64_t EVENT_TYPE = uint64_t(1)<<30;
#include "generation_metadata.inc"
#include "sample_metadata.inc"
constexpr uint64_t SAMPLE_TYPE = EVENT_TYPE+1;
// Signal-lane profiling samples: synthetic stacks, pooled stack traces whose
// frames reference pooled methods. All storage is sized at setup; nothing resizes.
constexpr size_t MAX_DEPTH = 1024, METHOD_COUNT = 4096, LANE_BUFFERS = 4, MAX_PRODUCERS = 64;
constexpr size_t HANDLER_SAMPLES = 65536, SNAPSHOT_BATCH_BYTES = 262144;
constexpr size_t MAX_NEST = 4; // signal nesting levels per thread, each with its own lane
constexpr uint16_t RAW_RECORD = 0x8000; // lane length-prefix bit: raw frames, not an encoded event
void big_endian(uint8_t* p,uint64_t v,size_t n=8) {for(size_t i=0;i<n;++i)p[n-i-1]=uint8_t(v>>(i*8));}
uint64_t now_ns() { return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count(); }
double cpu_seconds() {
    rusage r{}; if (getrusage(RUSAGE_SELF, &r)) throw std::runtime_error("getrusage");
    return r.ru_utime.tv_sec + r.ru_utime.tv_usec / 1e6 + r.ru_stime.tv_sec + r.ru_stime.tv_usec / 1e6;
}
uint8_t* put(uint8_t* p, uint64_t v) {
    for (int i=0; i<8 && v>=128; ++i) { *p++=uint8_t(v)|128; v>>=7; }
    *p++=uint8_t(v); return p;
}
uint64_t get(const uint8_t*& p, const uint8_t* end) {
    uint64_t v=0;
    for (int i=0;i<9;++i) {
        if (p==end) throw std::runtime_error("truncated varint");
        auto b=*p++; v|=uint64_t(i==8?b:b&127)<<(i*7);
        if (i==8 || b<128) return v;
    }
    throw std::runtime_error("varint");
}
uint64_t zigzag(int64_t v) { return (uint64_t(v)<<1) ^ (uint64_t(0)-uint64_t(v<0)); }
size_t size_frame(uint8_t* p, uint8_t* end) {
    auto n=size_t(end-p); if (n>=16384) throw std::runtime_error("frame too big");
    p[0]=uint8_t(n&127)|128; p[1]=uint8_t(n>>7); return n;
}
struct Config {
    std::string mode="generation", workload="handle";
    size_t producers=1, events=100000, cardinality=64, capacity=256;
    uint64_t rate=50000, rotate_us=0, stall_ms=0, pause_ms=0, drop_buffer_every=0;
    uint64_t flush_us=0, visibility_every=127, burst=64, flush_clock=0, drain_age_us=0;
    uint64_t signal_hz=0, stack_depth=32, stacks=256, nodefer=0, nest_levels=2, inject_every=0;
    uint64_t stack_capacity=4096, frame_capacity=1<<21, lane_kib=64;
    std::string stack_mode="global", stack_sharing="shared"; // accepted design: S3, two nesting levels
    bool observe_publication=false;
    bool streaming=false; // VIRL streaming chunks (spec §3.9.6): header written once, never patched
    bool finalize_on_close=false; // with streaming: finalize each chunk's header when it closes, if the sink can seek
    bool verify=false;
    std::string output;
};
using Value=std::array<uint8_t,VALUE_BYTES>;
uint64_t hash_value(const Value& v) {
    uint64_t h=1469598103934665603ULL;
    for (auto b:v) h=(h^b)*1099511628211ULL;
    return h;
}
struct Entry { Value value{}; uint64_t hash=0; uint32_t fixture=0; };
// Exactly one producer owns each pool slice. Background reads published entries
// only, after the corresponding buffer's ready or committed acquire; entries are immutable.
struct Pool {
    std::array<Entry,POOL_CAP> entries{};
    std::array<uint32_t,TABLE_CAP> table{};
    uint32_t count=0;
};
// Test-only re-entry injection: the outer handler arms one point; reaching it
// raises SIGPROF on this thread (raise is async-signal-safe; SA_NODEFER makes
// the nested handler run immediately, before the outer resumes).
static thread_local int tls_inject __attribute__((tls_model("initial-exec"))) = -1;
std::array<std::atomic<uint64_t>,4> sig_injected{};
inline void maybe_inject(int point) {
    if(tls_inject!=point)return;
    tls_inject=-1;sig_injected[size_t(point)].fetch_add(1,std::memory_order_relaxed);
    raise(SIGPROF);
}
struct StackFrame { uint32_t method=0, bci=0; };
struct StackRef { uint64_t hash=0; uint32_t depth=0, offset=0; };
uint64_t hash_stack(const StackFrame* f,uint32_t depth) {
    uint64_t h=0x84222325cbf29ce4ULL^depth;
    for(uint32_t i=0;i<depth;++i)h=(h^(uint64_t(f[i].method)<<32|f[i].bci))*0x9E3779B97F4A7C15ULL;
    return h^(h>>29);
}
// One stack table per generation; entries reference a fixed frame arena.
// Buckets are published with release stores (one writer) or release CAS
// (global mode, concurrent handlers); readers acquire bucket values and only
// see complete entries. Entries are immutable until the generation is reset,
// which happens only while no hazard or buffer references it.
struct StackTable {
    size_t capacity,frame_capacity,bucket_count;
    std::unique_ptr<StackRef[]> entries;
    std::unique_ptr<StackFrame[]> frames;
    std::unique_ptr<std::atomic<uint32_t>[]> buckets;
    std::unique_ptr<bool[]> emitted; // background-owned
    alignas(64) std::atomic<uint32_t> next_entry{0};
    alignas(64) std::atomic<uint32_t> next_frame{0};
    StackTable(size_t cap,size_t frame_cap):capacity(cap),frame_capacity(frame_cap),bucket_count(2*cap),
        entries(new StackRef[cap]),frames(new StackFrame[frame_cap]),buckets(new std::atomic<uint32_t>[2*cap]),
        emitted(new bool[cap]) { reset(); }
    void reset() {
        for(size_t i=0;i<bucket_count;++i)buckets[i].store(0,std::memory_order_relaxed);
        std::fill(emitted.get(),emitted.get()+capacity,false);
        next_entry.store(0,std::memory_order_relaxed);next_frame.store(0,std::memory_order_relaxed);
    }
    bool same(uint32_t key,uint64_t h,const StackFrame* f,uint32_t depth) const {
        const auto& e=entries[key-1];
        return e.hash==h&&e.depth==depth&&!std::memcmp(frames.get()+e.offset,f,depth*sizeof(StackFrame));
    }
    // Read-only lookup; async-signal-safe (hybrid handler).
    uint32_t find(uint64_t h,const StackFrame* f,uint32_t depth) const {
        size_t b=h%bucket_count;
        for(size_t j=0;j<PROBE_LIMIT;++j,b=(b+1)%bucket_count) {
            auto k=buckets[b].load(std::memory_order_acquire);
            if(!k)return 0;
            if(same(k,h,f,depth))return k;
        }
        return 0;
    }
    // Single-writer insert (background in stage/hybrid modes).
    uint32_t insert_owned(uint64_t h,const StackFrame* f,uint32_t depth) {
        size_t b=h%bucket_count;
        for(size_t j=0;j<PROBE_LIMIT;++j,b=(b+1)%bucket_count) {
            auto k=buckets[b].load(std::memory_order_relaxed);
            if(k){if(same(k,h,f,depth))return k;continue;}
            auto e=next_entry.load(std::memory_order_relaxed),o=next_frame.load(std::memory_order_relaxed);
            if(e>=capacity||o+depth>frame_capacity)return 0;
            entries[e]={h,depth,o};std::memcpy(frames.get()+o,f,depth*sizeof(StackFrame));
            next_entry.store(e+1,std::memory_order_relaxed);next_frame.store(o+depth,std::memory_order_relaxed);
            buckets[b].store(e+1,std::memory_order_release);
            return e+1;
        }
        return 0;
    }
    // Concurrent insert from signal handlers (global mode): reserve, fill, then
    // CAS-publish. A lost race reuses an equal winner; the loser's space is wasted.
    uint32_t insert_concurrent(uint64_t h,const StackFrame* f,uint32_t depth,uint64_t& lost,uint64_t& wasted) {
        size_t b=h%bucket_count;uint32_t mine=0;
        for(size_t j=0;j<PROBE_LIMIT;++j,b=(b+1)%bucket_count) {
            auto k=buckets[b].load(std::memory_order_acquire);
            if(!k) {
                if(!mine) {
                    auto e=next_entry.fetch_add(1,std::memory_order_relaxed);
                    if(e>=capacity)return 0;
                    auto o=next_frame.fetch_add(depth,std::memory_order_relaxed);
                    if(size_t(o)+depth>frame_capacity){++wasted;return 0;}
                    entries[e]={h,depth,o};std::memcpy(frames.get()+o,f,depth*sizeof(StackFrame));
                    mine=e+1;
                    maybe_inject(1); // reserved and filled, not yet published
                }
                uint32_t expected=0;
                if(buckets[b].compare_exchange_strong(expected,mine,std::memory_order_release,std::memory_order_acquire))return mine;
                ++lost;k=expected;
            }
            if(same(k,h,f,depth)){if(mine)++wasted;return k;}
        }
        if(mine)++wasted;
        return 0;
    }
};
// Which stack a lane samples: a per-lane seeded, skewed choice (cube of a
// uniform value), so a small hot set dominates as in real profiles and lanes
// are not synchronized: the hottest 1/8 of stacks get about half the samples.
uint64_t pick_stack(uint64_t seq,uint64_t lane,uint64_t stacks) {
    uint64_t z=seq*0x9E3779B97F4A7C15ULL^(lane+1)*0xD1B54A32D192ED03ULL;
    z=(z^(z>>30))*0xBF58476D1CE4E5B9ULL;z=(z^(z>>27))*0x94D049BB133111EBULL;z^=z>>31;
    const uint64_t u=z>>32,v=(((u*u)>>32)*u)>>32;
    return (v*stacks)>>32;
}
// Deterministic stand-in for a stack walk; the validator recomputes it.
// The root half is common to all stacks, as in real call trees.
void synth_stack(uint64_t stack,uint32_t depth,StackFrame* out) {
    for(uint32_t f=0;f<depth;++f) {
        out[f].method=uint32_t(f<depth/2?(f*7+1)%METHOD_COUNT:((stack*2654435761ULL+f*40503ULL)>>7)%METHOD_COUNT);
        out[f].bci=uint32_t((stack+f*3)%1000);
    }
}
struct Generation {
    uint64_t token=NO_GENERATION, anchor=0;
    bool open=false;
    std::vector<Pool> pools;
    std::vector<std::array<bool,POOL_CAP>> emitted;
    std::vector<std::array<uint32_t,POOL_CAP>> handle_keys;
    uint64_t accepted=0, body_bytes=0,last_snapshot=0,samples=0;
    std::unique_ptr<StackTable> stacks;
    std::array<bool,METHOD_COUNT> methods_emitted{};
    uint64_t file_offset=0;
    uint8_t publication_sequence=1;
    std::array<uint8_t,76> header{};
    Generation(size_t n):pools(n),emitted(n),handle_keys(n) {}
    void reset(uint64_t t,uint64_t a) {
        token=t; anchor=a; open=true; accepted=0;body_bytes=76;last_snapshot=0;publication_sequence=1;
        samples=0;methods_emitted.fill(false);
        if(stacks)stacks->reset();
        for (auto& p:pools) { p.count=0; p.table.fill(0); }
        for (auto& e:emitted) e.fill(false);
        for (auto& e:handle_keys)e.fill(0);
    }
};
struct Capture { uint64_t ticks=0; uint32_t end=0; }; // end: buffer offset after the sampled event
struct alignas(64) Buffer {
    std::atomic<bool> ready{false};
    // Producer release-publishes appended bytes; background may drain that
    // prefix of an unsealed buffer. Background resets it before releasing.
    std::atomic<size_t> committed{0};
    uint64_t token=NO_GENERATION; // written before the first committed release only
    size_t used=0;
    std::array<bool,POOL_CAP> referenced{}; // read by background only after seal
    std::atomic<size_t> capture_count{0};
    std::array<Capture,CAPTURE_SAMPLES> captures{};
    std::array<uint8_t,BUFFER_BYTES> bytes{};
};
struct Cache { uint64_t token=NO_GENERATION; uint32_t key=0; };
struct Producer {
    std::array<Buffer,BUFFER_COUNT> buffers;
    alignas(64) std::atomic<uint64_t> hazard{NO_GENERATION};
    std::atomic<bool> done{false};
    size_t index=0,cursor=0;
    uint64_t token=NO_GENERATION;
    uint64_t accepted=0,dropped=0,queue_drops=0,acquire_drops=0,post_pause_accepted=0;
    uint64_t hits=0,misses=0,fallbacks=0,probes=0,handoffs=0,intermediate=0;
    uint64_t flush_deadline=0,periodic_flushes=0,capture_samples=0;
    std::vector<Cache> cache;
    std::vector<uint64_t> success,drop,hit_latency,miss_latency,handoff_latency;
    Producer(size_t cardinality,size_t events):cache(cardinality) {
        // Worst-case targeted sampling stays allocation-free, even every call
        // being a miss or handoff. Sample storage is a harness-only budget.
        for (auto* v:{&success,&drop,&hit_latency,&miss_latency,&handoff_latency}) v->reserve(events);
    }
    void seal() {
        if (!cursor) return;
        auto& b=buffers[index]; b.used=cursor;
        b.ready.store(true,std::memory_order_release);
        index=(index+1)%BUFFER_COUNT; cursor=0;flush_deadline=0;
    }
    bool reserve(size_t n) {
        auto* b=&buffers[index];
        if (b->ready.load(std::memory_order_acquire)) return false;
        if (cursor+n>BUFFER_BYTES) {
            seal(); b=&buffers[index];
            if (b->ready.load(std::memory_order_acquire)) return false;
        }
        return true;
    }
    void append(const uint8_t* p,size_t n,uint32_t key,uint64_t capture,bool observe,uint64_t flush_us) {
        auto& b=buffers[index];
        if(!cursor) {
            b.referenced.fill(false);b.capture_count.store(0,std::memory_order_relaxed);
            b.token=token;flush_deadline=capture+flush_us*1000;
        }
        if(key)b.referenced[key-1]=true;
        uint16_t len=uint16_t(n);
        std::memcpy(b.bytes.data()+cursor,&len,2);
        std::memcpy(b.bytes.data()+cursor+2,p,n); cursor+=n+2;
        intermediate+=n+2;
        if(observe) {
            auto k=b.capture_count.load(std::memory_order_relaxed);
            if(k==CAPTURE_SAMPLES)throw std::runtime_error("capture sample overflow");
            b.captures[k]={capture,uint32_t(cursor)};b.capture_count.store(k+1,std::memory_order_release);++capture_samples;
        }
        b.committed.store(cursor,std::memory_order_release);
    }
};
struct Raw { uint64_t ticks,sequence; uint32_t key,fixture; }; // staged resolved reference
// Raw lane record: header, then depth frames. hashed: the handler already
// hashed the frames (hybrid miss); otherwise the background hashes them.
struct RawSample { uint64_t ticks,sequence,hash; uint32_t depth,hashed; };
struct LaneBuffer {
    std::atomic<bool> ready{false};
    std::atomic<size_t> committed{0};
    uint64_t token=NO_GENERATION; // written before the first committed release only
    size_t used=0;
    std::unique_ptr<uint8_t[]> bytes;
};
// A per-thread lane written only from that thread's signal handler: separate
// buffers and hazard slot, so a handler never touches state its interrupted
// thread may be mutating. A handler nested in another handler on the same
// thread uses the next level's lane (ThreadLanes); lanes are never shared.
struct Lane {
    std::array<LaneBuffer,LANE_BUFFERS> buffers;
    alignas(64) std::atomic<uint64_t> hazard{NO_GENERATION};
    std::atomic<int> state{0}; // 0 unregistered, 1 active, 2 retired
    std::atomic<uint64_t> nested{0},inactive{0};
    size_t id=0,level=0,index=0,cursor=0,capacity=0;
    uint64_t token=NO_GENERATION,seq=0;
    uint64_t entered=0,accepted=0,queue_drops=0,acquire_drops=0,pool_drops=0,interrupted=0,intermediate=0,handoffs=0;
    uint64_t cas_retries=0,wasted_entries=0,hits=0,misses=0,raw_records=0;
    size_t handler_count=0;
    std::array<uint32_t,HANDLER_SAMPLES> handler_ns{};
    explicit Lane(size_t bytes):capacity(bytes) {for(auto& b:buffers)b.bytes.reset(new uint8_t[bytes]);}
    void seal() {
        if(!cursor)return;
        auto& b=buffers[index];b.used=cursor;b.ready.store(true,std::memory_order_release);
        index=(index+1)%LANE_BUFFERS;cursor=0;
    }
    bool reserve(size_t n) {
        auto* b=&buffers[index];
        if(b->ready.load(std::memory_order_acquire))return false;
        if(cursor+n>capacity){seal();b=&buffers[index];if(b->ready.load(std::memory_order_acquire))return false;}
        return true;
    }
    // Two-part append so raw frames are copied once, straight into the lane.
    void append(uint16_t kind,const void* a,size_t na,const void* z=nullptr,size_t nz=0) {
        auto& b=buffers[index];
        if(!cursor)b.token=token;
        uint16_t len=uint16_t(kind|(na+nz));
        std::memcpy(b.bytes.get()+cursor,&len,2);std::memcpy(b.bytes.get()+cursor+2,a,na);
        if(nz)std::memcpy(b.bytes.get()+cursor+2+na,z,nz);
        maybe_inject(3); // bytes copied, cursor and committed not yet advanced
        cursor+=2+na+nz;intermediate+=2+na+nz;
        b.committed.store(cursor,std::memory_order_release);
    }
};
struct Sink {
    std::array<uint8_t,65536> bytes{}; size_t cursor=0;
    uint64_t wire_bytes=0,checksum=0,events=0,definitions=0,chunks=0,publications=0;
    uint64_t finalized=0,finalize_skipped=0;
    FILE* file=nullptr;
    std::array<std::array<std::atomic<uint8_t>,76>,2> published{};
    ~Sink(){if(file)std::fclose(file);}
    void patch(Generation& g) {
        auto& h=published[g.token%2];h[64].store(255);
        for(size_t i=0;i<76;++i)if(i!=64)h[i].store(g.header[i],std::memory_order_relaxed);
        h[64].store(g.header[64]);++publications;
        if(file) {
            auto end=ftello(file);
            if(fseeko(file,off_t(g.file_offset),SEEK_SET)||std::fwrite(g.header.data(),1,76,file)!=76||fseeko(file,end,SEEK_SET))throw std::runtime_error("header write");
        }
    }
    // Finalize-on-close (spec §3.9.6): one seek back per closed chunk. Posthoc fields
    // (bytes 8..63) first, then the flags byte with STREAMING cleared, so a concurrent
    // reader never sees size 0 with STREAMING clear. A sink that cannot seek (pipe, FIFO)
    // keeps the chunk streaming; that is counted, not an error.
    bool finalize(const std::array<uint8_t,76>& header,uint64_t file_offset) {
        if(!file){++finalize_skipped;return false;}
        if(std::fflush(file))throw std::runtime_error("output flush");
        const auto end=ftello(file);
        if(end<0||fseeko(file,off_t(file_offset+8),SEEK_SET)) {std::clearerr(file);++finalize_skipped;return false;}
        if(std::fwrite(header.data()+8,1,56,file)!=56||std::fflush(file)||
           fseeko(file,off_t(file_offset+67),SEEK_SET)||std::fwrite(header.data()+67,1,1,file)!=1||
           std::fflush(file)||fseeko(file,end,SEEK_SET))throw std::runtime_error("finalize write");
        ++finalized;return true;
    }
    void write(const uint8_t* p,size_t n) {
        // Memory sink: a ring standing in for consumed output; writes larger
        // than the ring (deep-stack snapshots) wrap in pieces.
        for(size_t off=0;off<n;) {
            if(cursor==bytes.size())cursor=0;
            const auto k=std::min(n-off,bytes.size()-cursor);
            std::memcpy(bytes.data()+cursor,p+off,k);cursor+=k;off+=k;
        }
        wire_bytes+=n;
        if(file&&std::fwrite(p,1,n,file)!=n)throw std::runtime_error("output write");
        checksum=checksum*33+p[0]+p[n-1];
    }
};
size_t event_frame(uint8_t* out,uint64_t ticks,uint64_t anchor,uint64_t seq,size_t id,
                   uint32_t key,const Value& value) {
    auto* p=put(out+2,EVENT_TYPE);
    auto delta=ticks>=anchor?int64_t(ticks-anchor):-int64_t(anchor-ticks);
    p=put(p,zigzag(delta)); p=put(p,seq); p=put(p,id);
    if (key) { *p++=2; p=put(p,id*POOL_CAP+key); }
    else { *p++=3; p=put(p,VALUE_BYTES); std::memcpy(p,value.data(),VALUE_BYTES); p+=VALUE_BYTES; }
    return size_frame(out,p);
}
size_t sample_frame(uint8_t* out,uint64_t ticks,uint64_t anchor,uint64_t seq,size_t id,uint64_t key) {
    auto* p=put(out+2,SAMPLE_TYPE);
    auto delta=ticks>=anchor?int64_t(ticks-anchor):-int64_t(anchor-ticks);
    p=put(p,zigzag(delta));p=put(p,seq);p=put(p,id);p=put(p,key);
    return size_frame(out,p);
}
// Signal context is fixed before the first signal and read-only afterwards.
enum class StackMode { stage, global, hybrid };
struct SignalContext {
    const Config* c=nullptr;
    std::array<std::unique_ptr<Generation>,2>* generations=nullptr;
    std::atomic<uint64_t>* active=nullptr;
    StackMode mode=StackMode::stage;
    bool fixed=false,disjoint=false;
};
SignalContext sig_ctx;
std::atomic<uint64_t> sig_unregistered{0};
// Per-thread nesting: depth is claimed with an atomic RMW, so a handler that
// interrupts another (even mid-claim) gets the next level; nested handlers
// always finish before the one they interrupted resumes.
struct ThreadLanes {
    std::atomic<uint32_t> depth{0};
    std::array<Lane*,MAX_NEST> levels{};
};
static thread_local ThreadLanes* tls_lanes __attribute__((tls_model("initial-exec"))) = nullptr;
static_assert(std::atomic<bool>::is_always_lock_free&&std::atomic<uint64_t>::is_always_lock_free&&
              std::atomic<uint32_t>::is_always_lock_free&&std::atomic<size_t>::is_always_lock_free,"signal-safe atomics");
// Runs in signal context: lock-free atomics, clock reads and fixed storage only.
void record_sample(Lane& lane,uint64_t capture) {
    const auto& ctx=sig_ctx;const auto& c=*ctx.c;
    const uint64_t seq=lane.seq++;
    maybe_inject(0);
    const uint64_t pick=pick_stack(seq,lane.id,c.stacks);
    const uint64_t stack=ctx.disjoint?lane.id*c.stacks+pick:pick;
    const auto depth=uint32_t(c.stack_depth);
    StackFrame frames[MAX_DEPTH];
    synth_stack(stack,depth,frames); // stands in for the stack walk
    const uint64_t wanted=ctx.fixed?0:ctx.active->load();
    if(lane.token!=wanted) {
        // Same SC hazard protocol as producers, on the lane's own slot.
        ++lane.handoffs;lane.seal();lane.hazard.store(NO_GENERATION);
        lane.hazard.store(wanted);
        if(!ctx.fixed&&ctx.active->load()!=wanted){lane.hazard.store(NO_GENERATION);lane.token=NO_GENERATION;++lane.acquire_drops;return;}
        lane.token=wanted;
    }
    auto& g=*(*ctx.generations)[lane.token%2];
    auto& table=*g.stacks;
    uint32_t key=0;uint64_t h=0;bool hashed=false;
    if(ctx.mode!=StackMode::stage) {
        h=hash_stack(frames,depth);hashed=true;
        if(ctx.mode==StackMode::global) {
            key=table.insert_concurrent(h,frames,depth,lane.cas_retries,lane.wasted_entries);
            // A pooled stack field has no inline fallback: exhaustion drops the sample.
            if(!key){++lane.pool_drops;return;}
        }else if((key=table.find(h,frames,depth)))++lane.hits;
        else ++lane.misses;
    }
    if(key) {
        uint8_t record[64];
        const auto n=sample_frame(record,capture,g.anchor,seq,lane.id,key);
        if(!lane.reserve(2+n)){++lane.queue_drops;return;}
        maybe_inject(2); // space checked, not yet appended
        lane.append(0,record,n);
    }else {
        const size_t n=depth*sizeof(StackFrame);
        if(!lane.reserve(2+sizeof(RawSample)+n)){++lane.queue_drops;return;}
        maybe_inject(2);
        const RawSample raw{capture,seq,h,depth,hashed?1u:0u};
        lane.append(RAW_RECORD,&raw,sizeof(raw),frames,n);++lane.raw_records;
    }
    ++lane.accepted;
}
void on_sample(int,siginfo_t*,void*) {
    const int saved_errno=errno;
    ThreadLanes* lanes=tls_lanes;
    if(!lanes){sig_unregistered.fetch_add(1,std::memory_order_relaxed);errno=saved_errno;return;}
    const auto level=lanes->depth.fetch_add(1,std::memory_order_acquire);
    if(level>=sig_ctx.c->nest_levels) {
        // Deeper than the configured levels: drop and count; never share a lane.
        lanes->levels[0]->nested.fetch_add(1,std::memory_order_relaxed);
        lanes->depth.fetch_sub(1,std::memory_order_release);errno=saved_errno;return;
    }
    Lane* lane=lanes->levels[level];
    if(lane->state.load(std::memory_order_acquire)!=1) {
        lane->inactive.fetch_add(1,std::memory_order_relaxed);
        lanes->depth.fetch_sub(1,std::memory_order_release);errno=saved_errno;return;
    }
    std::atomic_signal_fence(std::memory_order_seq_cst);
    const bool was_recording=recording_call;recording_call=true;
    if(was_recording)++lane->interrupted;
    const auto every=sig_ctx.c->inject_every;
    if(every&&level==0&&lane->seq%every==every-1)tls_inject=int((lane->seq/every)%4);
    const auto begin=now_ns();++lane->entered;
    record_sample(*lane,begin);
    if(level==0)tls_inject=-1; // an armed point this sample never reached
    const auto spent=now_ns()-begin;
    if(lane->handler_count<HANDLER_SAMPLES)lane->handler_ns[lane->handler_count++]=uint32_t(std::min<uint64_t>(spent,UINT32_MAX));
    recording_call=was_recording;
    std::atomic_signal_fence(std::memory_order_seq_cst);
    lanes->depth.fetch_sub(1,std::memory_order_release);
    errno=saved_errno;
}
uint64_t percentile(std::vector<uint64_t>& v,double q) {
    if(v.empty())return 0;
    std::sort(v.begin(),v.end()); return v[size_t(q*(v.size()-1))];
}

int main(int argc,char** argv) try {
    Config c;
    for(int i=1;i<argc;++i) {
        std::string a=argv[i];
        if(a=="--verify") { c.verify=true; continue; }
        if(++i==argc)throw std::runtime_error("missing value");
        std::string v=argv[i];
        if(a=="--output") {c.output=v;continue;}
        if(a=="--mode") {c.mode=v;continue;}
        if(a=="--workload") {c.workload=v;continue;}
        if(a=="--stack-mode") {c.stack_mode=v;continue;}
        if(a=="--stack-sharing") {c.stack_sharing=v;continue;}
        if(v.empty()||v[0]=='-')throw std::runtime_error("invalid number");
        size_t end=0; auto n=std::stoull(v,&end);
        if(end!=v.size())throw std::runtime_error("invalid number");
        if(a=="--producers")c.producers=n;
        else if(a=="--events")c.events=n;
        else if(a=="--cardinality")c.cardinality=n;
        else if(a=="--capacity")c.capacity=n;
        else if(a=="--rate")c.rate=n;
        else if(a=="--rotate-us")c.rotate_us=n;
        else if(a=="--stall-ms")c.stall_ms=n;
        else if(a=="--pause-ms")c.pause_ms=n;
        else if(a=="--drop-buffer-every")c.drop_buffer_every=n;
        else if(a=="--flush-us")c.flush_us=n;
        else if(a=="--flush-clock")c.flush_clock=n;
        else if(a=="--observe-publication")c.observe_publication=n!=0;
        else if(a=="--streaming")c.streaming=n!=0;
        else if(a=="--finalize-on-close")c.finalize_on_close=n!=0;
        else if(a=="--visibility-every")c.visibility_every=n;
        else if(a=="--burst")c.burst=n;
        else if(a=="--drain-age-us")c.drain_age_us=n;
        else if(a=="--signal-hz")c.signal_hz=n;
        else if(a=="--stack-depth")c.stack_depth=n;
        else if(a=="--stacks")c.stacks=n;
        else if(a=="--nodefer")c.nodefer=n;
        else if(a=="--nest-levels")c.nest_levels=n;
        else if(a=="--inject-every")c.inject_every=n;
        else if(a=="--stack-capacity")c.stack_capacity=n;
        else if(a=="--frame-capacity")c.frame_capacity=n;
        else if(a=="--lane-kib")c.lane_kib=n;
        else throw std::runtime_error("unknown option");
    }
    if((c.mode!="fixed"&&c.mode!="generation"&&c.mode!="staged") ||
       (c.workload!="handle"&&c.workload!="lookup"&&c.workload!="miss") ||
       !c.producers||c.producers>MAX_PRODUCERS||!c.events||c.events>10000000||
       !c.cardinality||c.cardinality>65536||!c.capacity||c.capacity>POOL_CAP||
       c.rate>1000000000||c.stall_ms>1000||c.pause_ms>1000||
       c.flush_us>10000000||c.flush_clock>1||!c.visibility_every||c.visibility_every>1000000||
       !c.burst||c.burst>65536||c.drain_age_us>10000000||c.signal_hz>100000||!c.stack_depth||c.stack_depth>MAX_DEPTH||
       !c.stacks||c.stacks>65536||c.nodefer>1||!c.nest_levels||c.nest_levels>MAX_NEST||
       (c.inject_every&&!c.nodefer)||c.inject_every>1000000||(c.stack_mode!="stage"&&c.stack_mode!="global"&&c.stack_mode!="hybrid")||
       !c.stack_capacity||c.stack_capacity>65536||c.frame_capacity<MAX_DEPTH||c.frame_capacity>(1u<<24)||
       c.lane_kib<16||c.lane_kib>1024||
       (c.stack_sharing!="shared"&&c.stack_sharing!="disjoint")||c.rotate_us>10000000||(c.mode=="fixed"&&c.rotate_us)||(c.finalize_on_close&&!c.streaming))throw std::runtime_error("invalid configuration");
    std::vector<Value> values(c.cardinality);
    for(size_t i=0;i<values.size();++i) {
        uint64_t x=i+1;
        for(auto& b:values[i]){x=x*6364136223846793005ULL+1;b=uint8_t('!'+(x>>32)%90);}
    }
    std::array<std::unique_ptr<Generation>,2> generations{
        std::make_unique<Generation>(c.producers),std::make_unique<Generation>(c.producers)};
    std::vector<std::unique_ptr<Producer>> producers;
    for(size_t i=0;i<c.producers;++i)producers.push_back(std::make_unique<Producer>(c.cardinality,c.events));
    const bool signals=c.signal_hz!=0;
    const auto stack_mode=c.stack_mode=="global"?StackMode::global:c.stack_mode=="hybrid"?StackMode::hybrid:StackMode::stage;
    const size_t lane_bytes=c.lane_kib*1024;
    if(signals&&2+sizeof(RawSample)+c.stack_depth*sizeof(StackFrame)>std::min<size_t>(lane_bytes,0x7fff))
        throw std::runtime_error("lane buffer cannot hold one raw sample");
    std::vector<std::unique_ptr<Lane>> lanes;
    // Lane id = level * producers + producer; it is the sample's producer field.
    if(signals)for(size_t level=0;level<c.nest_levels;++level)for(size_t i=0;i<c.producers;++i) {
        lanes.push_back(std::make_unique<Lane>(lane_bytes));lanes.back()->id=lanes.size()-1;lanes.back()->level=level;
    }
    std::unique_ptr<ThreadLanes[]> thread_lanes(new ThreadLanes[c.producers]);
    if(signals)for(size_t i=0;i<c.producers;++i)for(size_t level=0;level<c.nest_levels;++level)
        thread_lanes[i].levels[level]=lanes[level*c.producers+i].get();
    for(auto& g:generations)if(signals)g->stacks=std::make_unique<StackTable>(c.stack_capacity,c.frame_capacity);
    // Handle and lookup workloads start with registered definitions. Miss starts empty.
    auto seed=[&](Generation& g) {
        if(c.workload=="miss")return;
        for(size_t id=0;id<c.producers;++id)for(size_t i=0;i<std::min(c.cardinality,c.capacity);++i) {
            auto& pool=g.pools[id];auto h=hash_value(values[i]);size_t bucket=h%TABLE_CAP;
            for(size_t j=0;j<PROBE_LIMIT;++j,bucket=(bucket+1)%TABLE_CAP)if(!pool.table[bucket]) {
                auto key=++pool.count;pool.entries[key-1]={values[i],h,uint32_t(i)};pool.table[bucket]=key;g.handle_keys[id][i]=key;break;
            }
        }
    };
    generations[0]->reset(0,0);seed(*generations[0]);
    std::atomic<uint64_t> active{0};std::atomic<bool> go{false};std::atomic<size_t> ready{0};
    std::atomic<size_t> producers_finished{0};std::atomic<bool> sampler_stopped{!signals};
    sig_ctx.c=&c;sig_ctx.generations=&generations;sig_ctx.active=&active;
    sig_ctx.fixed=c.mode=="fixed";sig_ctx.mode=stack_mode;sig_ctx.disjoint=c.stack_sharing=="disjoint";
    if(signals) {
        struct sigaction sa{};sa.sa_sigaction=on_sample;sigemptyset(&sa.sa_mask);
        sa.sa_flags=SA_SIGINFO|SA_RESTART|(c.nodefer?SA_NODEFER:0);
        if(sigaction(SIGPROF,&sa,nullptr))throw std::runtime_error("sigaction");
    }
    uint64_t start=0;
    Sink sink;
    // Streaming output never seeks, so it may be a pipe or FIFO; finalized output back-patches headers.
    if(!c.output.empty()) {sink.file=std::fopen(c.output.c_str(),c.streaming?"wb":"wb+");if(!sink.file)throw std::runtime_error("open output");}
    const auto epoch_now=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    const auto epoch_ticks=now_ns();
    uint64_t rotations=0,rotation_deferred=0,max_live=1,max_queued=0,negative_offsets=0;
    uint64_t discarded_events=0,drained_buffers=0,max_defer_ns=0,discarded_capture_samples=0,partial_drains=0;
    uint64_t samples_committed=0,stage_pool_drops=0,lane_partial_drains=0,sample_event_bytes=0,sample_definition_bytes=0;
    uint64_t stack_definitions=0,method_definitions=0,sample_drain_ns=0,signals_sent=0;
    std::vector<uint64_t> publication_latency;
    std::exception_ptr consumer_error;
    std::vector<std::thread> threads;
    for(size_t id=0;id<c.producers;++id)threads.emplace_back([&,id] {
        auto& pr=*producers[id];std::array<uint8_t,128> record{};
        if(signals) {
            for(size_t level=0;level<c.nest_levels;++level)thread_lanes[id].levels[level]->state.store(1,std::memory_order_release);
            tls_lanes=&thread_lanes[id];
        }
        ready.fetch_add(1);while(!go.load(std::memory_order_acquire))std::this_thread::yield();
        measured_worker=true;
        for(size_t seq=0;seq<c.events;++seq) {
            if(c.rate&&seq%c.burst==0)std::this_thread::sleep_until(Clock::time_point(std::chrono::nanoseconds(start+uint64_t(double(seq)*1e9/c.rate))));
            bool handoff=false,miss=false,ok=false;
            // Sampling phase varies by producer; misses/handoffs additionally sampled
            // with their complete recording call. Measurement includes clock overhead.
            auto begin=now_ns();recording_call=true;
            const auto capture=now_ns();
            if(c.flush_us&&pr.cursor) {
                auto deadline_now=c.flush_clock?now_ns():capture;
                if(deadline_now>=pr.flush_deadline) {pr.seal();++pr.periodic_flushes;}
            }
            auto wanted=c.mode=="fixed"?0:active.load();
            if(pr.token!=wanted) {
                handoff=true;++pr.handoffs;pr.seal();pr.hazard.store(NO_GENERATION);
                // SC hazard publish + SC active recheck. Never dereference an
                // unprotected descriptor. Single retry-free attempt can drop.
                pr.hazard.store(wanted);
                if(c.mode!="fixed" && active.load()!=wanted) {
                    pr.hazard.store(NO_GENERATION);pr.token=NO_GENERATION;++pr.acquire_drops;
                }else pr.token=wanted;
            }
            if(pr.token!=NO_GENERATION) {
                auto& g=*generations[pr.token%2];
                // Pause while holding a live descriptor; included only in fault
                // scenario call latency, never interpreted as encoder performance.
                if(c.pause_ms&&id==0&&seq==c.events/4)std::this_thread::sleep_for(std::chrono::milliseconds(c.pause_ms));
                if(!pr.reserve(2+sizeof(Raw)+VALUE_BYTES+32))++pr.queue_drops;
                else {
                    auto& pool=g.pools[id];size_t fixture=seq%c.cardinality;
                    uint32_t key=0;auto& cache=pr.cache[fixture];
                    if(c.workload=="handle"&&cache.token==pr.token) {key=cache.key;++pr.hits;}
                    else {
                        // Registered handle binds its stable fixture directly on
                        // first use in a generation. Other paths hash/probe bytes.
                        if(c.workload=="handle") {
                            if(fixture<POOL_CAP)key=g.handle_keys[id][fixture];
                            if(key)++pr.hits;
                        }else {
                            auto h=hash_value(values[fixture]);size_t bucket=h%TABLE_CAP;
                            for(size_t j=0;j<PROBE_LIMIT;++j,bucket=(bucket+1)%TABLE_CAP) {
                                ++pr.probes;auto k=pool.table[bucket];
                                if(!k) {
                                    miss=true;++pr.misses;
                                    if(pool.count<c.capacity) {
                                        key=++pool.count;pool.entries[key-1]={values[fixture],h,uint32_t(fixture)};pool.table[bucket]=key;
                                    }
                                    break;
                                }
                                if(pool.entries[k-1].hash==h&&pool.entries[k-1].value==values[fixture]){key=k;++pr.hits;break;}
                            }
                        }
                        if(c.workload=="handle") { cache.token=pr.token;cache.key=key; }
                    }
                    if(!key)++pr.fallbacks;
                    auto ticks=capture; // Capture time precedes lookup/encoding; signed offsets permit a newer anchor.
                    if(c.verify&&seq%17==0)ticks=g.anchor-1;
                    size_t n=0;
                    if(c.mode=="staged") {
                        Raw raw{ticks,seq,key,uint32_t(fixture)};
                        std::memcpy(record.data(),&raw,sizeof(raw));n=sizeof(raw);
                        // A miss fallback captures bytes; the consumer may not
                        // dereference an application-owned fixture as staged data.
                        if(!key){std::memcpy(record.data()+n,values[fixture].data(),VALUE_BYTES);n+=VALUE_BYTES;}
                    }else n=event_frame(record.data(),ticks,g.anchor,seq,id,key,values[fixture]);
                    pr.append(record.data(),n,key,capture,c.observe_publication&&(seq+id*17)%c.visibility_every==0,c.flush_us);ok=true;
                }
            }
            recording_call=false;auto duration=now_ns()-begin;
            if(ok) {++pr.accepted;if(c.pause_ms&&id==0&&seq>c.events/4)++pr.post_pause_accepted;}else ++pr.dropped;
            if((seq+id*17)%127==0)(ok?pr.success:pr.drop).push_back(duration);
            if(ok&&miss)pr.miss_latency.push_back(duration);
            if(ok&&handoff)pr.handoff_latency.push_back(duration);
            if(ok&&!miss&&!handoff&&(seq+id*17)%127==0)pr.hit_latency.push_back(duration);
        }
        if(signals) {
            // Keep receiving samples until the sampler stops (pthread_kill needs a
            // live thread), then block the signal: no handler can run after this,
            // so this thread may finish the lane as its last writer.
            producers_finished.fetch_add(1);
            while(!sampler_stopped.load(std::memory_order_acquire))std::this_thread::yield();
            sigset_t set;sigemptyset(&set);sigaddset(&set,SIGPROF);pthread_sigmask(SIG_BLOCK,&set,nullptr);
            std::atomic_signal_fence(std::memory_order_seq_cst);
            for(size_t level=0;level<c.nest_levels;++level) {
                auto& lane=*thread_lanes[id].levels[level];lane.state.store(2,std::memory_order_release);
                lane.seal();lane.hazard.store(NO_GENERATION);
            }
            tls_lanes=nullptr;
        }
        pr.seal();pr.hazard.store(NO_GENERATION);pr.done.store(true,std::memory_order_release);
        measured_worker=false;
    });
    std::thread consumer([&] {
        std::vector<size_t> read_index(c.producers,0),drained(c.producers,0),sample_index(c.producers,0);
        std::vector<char> started(c.producers,0),discarding(c.producers,0);
        std::array<bool,POOL_CAP> prefix_referenced{};
        std::vector<size_t> lane_read(lanes.size(),0),lane_drained(lanes.size(),0);
        std::vector<uint64_t> lane_previous(lanes.size(),NO_GENERATION);
        // Fixed background scratch, allocated before measurement starts.
        std::vector<uint8_t> stack_snapshot(SNAPSHOT_BATCH_BYTES+MAX_DEPTH*12+METHOD_COUNT*16);
        std::vector<uint32_t> method_list(METHOD_COUNT),span_keys(lane_bytes/8+1),fresh(c.stack_capacity);
        std::vector<StackFrame> raw_frames(MAX_DEPTH);
        if(c.observe_publication)publication_latency.reserve(c.events*c.producers);
        std::vector<uint64_t> previous(c.producers,NO_GENERATION);
        std::array<uint8_t,256> output{};
        auto publish=[&](Generation& g,bool complete=false) {
            // Streaming chunks keep their first header: extent comes from the END_OF_PARTITION
            // event, summaries from in-band state snapshots.
            if(c.streaming)return;
            big_endian(g.header.data()+8,g.body_bytes);
            big_endian(g.header.data()+16,g.last_snapshot);
            big_endian(g.header.data()+40,now_ns()-g.anchor);
            g.header[64]=complete?0:g.publication_sequence;
            if(!complete)g.publication_sequence=g.publication_sequence==254?1:uint8_t(g.publication_sequence+1);
            sink.patch(g);
        };
        auto metadata=[&](Generation& g) {
            g.header.fill(0);std::memcpy(g.header.data(),"VIRL",4);
            big_endian(g.header.data()+6,1,2); // draft 0.1
            big_endian(g.header.data()+8,76);big_endian(g.header.data()+24,76);
            big_endian(g.header.data()+32,uint64_t(epoch_now)+g.anchor-epoch_ticks);
            big_endian(g.header.data()+48,g.anchor);big_endian(g.header.data()+56,1000000000);
            big_endian(g.header.data()+68,0x564952454c42454eULL); // one logical writer
            g.header[64]=1;g.header[66]=128;g.header[67]=1;
            if(c.streaming) {big_endian(g.header.data()+8,0);big_endian(g.header.data()+24,76);g.header[64]=0;g.header[67]=1|4;}
            g.file_offset=sink.wire_bytes;sink.write(g.header.data(),76);
            const auto* schema=signals?SAMPLE_METADATA:NATIVE_METADATA;
            const size_t schema_size=signals?sizeof(SAMPLE_METADATA):sizeof(NATIVE_METADATA);
            sink.write(schema,schema_size);g.body_bytes+=schema_size;++sink.chunks;
            publish(g);
        };
        auto snapshot=[&](Generation& g,size_t id,const bool* referenced,bool summary) {
            std::array<uint8_t,32768> data;auto* q=put(data.data()+3,1);
            q=put(q,0);q=put(q,0);
            q=put(q,g.last_snapshot?uint64_t(g.last_snapshot-g.body_bytes):0);
            *q++=summary?129:1;
            if(summary) {
                q=put(q,1);q=put(q,(g.accepted?1:0)+(g.samples?1:0));
                if(g.accepted){q=put(q,EVENT_TYPE);q=put(q,g.accepted);}
                if(g.samples){q=put(q,SAMPLE_TYPE);q=put(q,g.samples);}
                q=put(q,0);
            }else {
                size_t count=0;for(size_t k=0;k<c.capacity;++k)
                    if(referenced[k]&&!g.emitted[id][k])++count;
                if(!count)return;
                q=put(q,1);q=put(q,101);q=put(q,count);
                for(size_t k=0;k<c.capacity;++k)if(referenced[k]&&!g.emitted[id][k]) {
                    q=put(q,id*POOL_CAP+k+1);*q++=3;q=put(q,VALUE_BYTES);
                    std::memcpy(q,g.pools[id].entries[k].value.data(),VALUE_BYTES);q+=VALUE_BYTES;
                    g.emitted[id][k]=true;++sink.definitions;
                }
            }
            size_t n=size_t(q-data.data());
            data[0]=uint8_t(n&127)|128;data[1]=uint8_t((n>>7)&127)|128;data[2]=uint8_t(n>>14);
            g.last_snapshot=g.body_bytes;sink.write(data.data(),n);g.body_bytes+=n;
        };
        auto close=[&](Generation& g,bool last=false) {
            snapshot(g,0,nullptr,true); // exact output-committed events; eos follows
            uint8_t eos[]={128|3,0,8};sink.write(eos,3);g.body_bytes+=3;
            if(c.streaming&&c.finalize_on_close) {
                big_endian(g.header.data()+8,g.body_bytes);
                big_endian(g.header.data()+16,g.last_snapshot);
                big_endian(g.header.data()+40,now_ns()-g.anchor);
                g.header[67]=uint8_t((g.header[67]&~4)|(last?2:0)); // STREAMING cleared; FINAL-CHUNK on the last
                if(!sink.finalize(g.header,g.file_offset)) // stays streaming: restore the written header
                    {big_endian(g.header.data()+8,0);big_endian(g.header.data()+16,0);big_endian(g.header.data()+40,0);g.header[67]=1|4;}
            }
            publish(g,true);g.open=false;
        };
        // Producer-buffer record helpers for unsealed prefixes: the reference
        // bitmap is still being written, so keys and capture ticks are parsed.
        auto record_key=[&](const uint8_t* wire,size_t n,size_t id)->uint32_t {
            if(c.mode=="staged") {Raw raw{};std::memcpy(&raw,wire,sizeof(raw));return raw.key;}
            const auto* q=wire;auto* end=wire+n;
            for(int i=0;i<5;++i)get(q,end); // size, type, start, sequence, producer
            if(*q++!=2)return 0;
            return uint32_t(get(q,end)-id*POOL_CAP);
        };
        auto record_ticks=[&](const uint8_t* wire,size_t n,const Generation& g)->uint64_t {
            if(c.mode=="staged") {Raw raw{};std::memcpy(&raw,wire,sizeof(raw));return raw.ticks;}
            const auto* q=wire;auto* end=wire+n;get(q,end);get(q,end);
            auto z=get(q,end);auto offset=int64_t(z>>1)^-int64_t(z&1);
            return g.anchor+uint64_t(offset);
        };
        // Background symbolization stand-in: methods are defined before the
        // stacks that reference them; snapshot frames are bounded by bytes.
        auto emit_stacks=[&](Generation& g,size_t count) {
            const auto& t=*g.stacks;
            for(size_t first=0;first<count;) {
                size_t last=first,estimate=0,methods=0;
                while(last<count&&(last==first||estimate<SNAPSHOT_BATCH_BYTES)) {
                    const auto& e=t.entries[fresh[last]];
                    for(uint32_t f=0;f<e.depth;++f) {
                        auto m=t.frames[e.offset+f].method;
                        if(!g.methods_emitted[m]){g.methods_emitted[m]=true;method_list[methods++]=m;estimate+=16;}
                    }
                    estimate+=8+e.depth*8;++last;
                }
                auto* q=put(stack_snapshot.data()+3,1);q=put(q,0);q=put(q,0);
                q=put(q,g.last_snapshot?uint64_t(g.last_snapshot-g.body_bytes):0);*q++=1;
                q=put(q,methods?2:1);
                if(methods) {
                    q=put(q,102);q=put(q,methods);
                    for(size_t i=0;i<methods;++i) {
                        char name[16];auto len=std::snprintf(name,sizeof(name),"m%u",method_list[i]);
                        q=put(q,method_list[i]+1);*q++=3;q=put(q,uint64_t(len));std::memcpy(q,name,size_t(len));q+=len;
                    }
                }
                q=put(q,103);q=put(q,last-first);
                for(size_t i=first;i<last;++i) {
                    const auto& e=t.entries[fresh[i]];
                    q=put(q,fresh[i]+1);q=put(q,e.depth);
                    for(uint32_t f=0;f<e.depth;++f){q=put(q,t.frames[e.offset+f].method+1);q=put(q,t.frames[e.offset+f].bci);}
                }
                size_t n=size_t(q-stack_snapshot.data());
                if(n>=(1u<<21))throw std::runtime_error("snapshot too large");
                stack_snapshot[0]=uint8_t(n&127)|128;stack_snapshot[1]=uint8_t((n>>7)&127)|128;stack_snapshot[2]=uint8_t(n>>14);
                g.last_snapshot=g.body_bytes;sink.write(stack_snapshot.data(),n);g.body_bytes+=n;
                sample_definition_bytes+=n;stack_definitions+=last-first;method_definitions+=methods;
                first=last;
            }
        };
        auto lane_ticks=[&](const uint8_t* record,uint16_t prefix,const Generation& g)->uint64_t {
            if(prefix&RAW_RECORD){RawSample raw{};std::memcpy(&raw,record,sizeof(raw));return raw.ticks;}
            const auto* q=record;auto* end=record+(prefix&0x7fff);get(q,end);get(q,end);
            auto z=get(q,end);auto offset=int64_t(z>>1)^-int64_t(z&1);
            return g.anchor+uint64_t(offset);
        };
        auto write_sample=[&](Generation& g,const uint8_t* wire,size_t n,size_t id) {
            const auto* p=wire;auto* end=wire+n;
            if(get(p,end)!=n||get(p,end)!=SAMPLE_TYPE)throw std::runtime_error("sample frame");
            get(p,end);auto seq=get(p,end);
            if(get(p,end)!=id)throw std::runtime_error("sample producer");
            auto key=get(p,end);if(p!=end)throw std::runtime_error("sample trailing");
            if(lane_previous[id]!=NO_GENERATION&&seq<=lane_previous[id])throw std::runtime_error("sample order");
            lane_previous[id]=seq;
            if(!key||key>g.stacks->capacity||!g.stacks->emitted[key-1])throw std::runtime_error("undefined stack");
            sink.write(wire,n);g.body_bytes+=n;++g.samples;++samples_committed;sample_event_bytes+=n;
        };
        // Pass 1 resolves every record to a key (interning raw frames, single
        // writer) and collects keys not yet defined in this chunk; their
        // definitions are written before pass 2 writes any event. In hybrid mode
        // a handler can hit a key only after this thread published its bucket,
        // and this thread finishes the definitions before draining anything else.
        auto drain_lane=[&](Generation& g,size_t id,const LaneBuffer& b,size_t from,size_t limit) {
            auto& t=*g.stacks;size_t records=0,count=0;
            auto define=[&](uint32_t key){if(!t.emitted[key-1]){t.emitted[key-1]=true;fresh[count++]=key-1;}};
            for(size_t pos=from;pos<limit;++records) {
                uint16_t prefix=0;std::memcpy(&prefix,b.bytes.get()+pos,2);pos+=2;
                const size_t length=prefix&0x7fff;
                if(pos+length>limit)throw std::runtime_error("lane length");
                const auto* record=b.bytes.get()+pos;uint32_t key=0;
                if(prefix&RAW_RECORD) {
                    if(stack_mode==StackMode::global)throw std::runtime_error("raw record in global mode");
                    RawSample raw{};
                    if(length<sizeof(raw))throw std::runtime_error("raw length");
                    std::memcpy(&raw,record,sizeof(raw));
                    if(!raw.depth||raw.depth>MAX_DEPTH||length!=sizeof(raw)+raw.depth*sizeof(StackFrame))throw std::runtime_error("raw sample");
                    std::memcpy(raw_frames.data(),record+sizeof(raw),raw.depth*sizeof(StackFrame));
                    const auto h=raw.hashed?raw.hash:hash_stack(raw_frames.data(),raw.depth);
                    key=t.insert_owned(h,raw_frames.data(),raw.depth);
                    if(!key)++stage_pool_drops;
                }else {
                    if(stack_mode==StackMode::stage)throw std::runtime_error("key record in stage mode");
                    const auto* q=record;auto* end=q+length;
                    for(int i=0;i<5;++i)get(q,end); // size, type, start, sequence, producer
                    key=uint32_t(get(q,end));
                    if(!key||key>t.capacity)throw std::runtime_error("stack key");
                }
                if(key)define(key);
                span_keys[records]=key;pos+=length;
            }
            emit_stacks(g,count);
            records=0;
            for(size_t pos=from;pos<limit;++records) {
                uint16_t prefix=0;std::memcpy(&prefix,b.bytes.get()+pos,2);pos+=2;
                const size_t length=prefix&0x7fff;const auto* record=b.bytes.get()+pos;
                if(!(prefix&RAW_RECORD))write_sample(g,record,length,id);
                else if(auto key=span_keys[records]) {
                    RawSample raw{};std::memcpy(&raw,record,sizeof(raw));
                    auto n=sample_frame(output.data(),raw.ticks,g.anchor,raw.sequence,id,key);
                    write_sample(g,output.data(),n,id);
                }
                pos+=length;
            }
        };
        auto pinned=[&](uint64_t token) {
            for(auto& l:lanes)if(l->hazard.load()==token)return true;
            for(auto& l:lanes)for(auto& b:l->buffers)
                if(b.ready.load(std::memory_order_acquire)&&b.token==token)return true;
            // active was changed before this scan. A racing new hazard must
            // fail its active recheck before reading this retired descriptor.
            for(auto& p:producers)if(p->hazard.load()==token)return true;
            // Producer seals its buffer before clearing the hazard. Acquire
            // ready sees that publication when the SC hazard scan saw release.
            for(auto& p:producers)for(auto& b:p->buffers)
                if(b.ready.load(std::memory_order_acquire)&&b.token==token)return true;
            return false;
        };
        ready.fetch_add(1);while(!go.load(std::memory_order_acquire))std::this_thread::yield();
        measured_worker=true;
        try {
            metadata(*generations[0]);
            auto next_rotation=start+c.rotate_us*1000;
            auto stall_end=start+c.stall_ms*1000000;
            uint64_t emitting=0,defer_start=0;
            bool stalled=c.stall_ms!=0;
            for(;;) {
                auto now=now_ns();
                if(c.rotate_us&&now>=next_rotation) {
                    auto old=active.load();auto& next=*generations[(old+1)%2];
                    if(!next.open) {
                        next.reset(old+1,now);seed(next);active.store(old+1);++rotations;
                        if(defer_start) {max_defer_ns=std::max(max_defer_ns,now-defer_start);defer_start=0;}
                        max_live=2;next_rotation=now+c.rotate_us*1000;
                    }else {++rotation_deferred;if(!defer_start)defer_start=now;next_rotation=now+100000;}
                }
                if(stalled&&now>=stall_end)stalled=false;
                size_t queued=0;for(auto& p:producers)for(auto& b:p->buffers)if(b.ready.load(std::memory_order_acquire))++queued;
                max_queued=std::max(max_queued,uint64_t(queued));
                bool all_done=true,progress=false;
                auto& g=*generations[emitting%2];
                for(size_t id=0;id<c.producers;++id) {
                    auto& pr=*producers[id];bool done=pr.done.load(std::memory_order_acquire);
                    if(!done)all_done=false;
                    auto& b=pr.buffers[read_index[id]];
                    if(stalled)continue;
                    const bool sealed=b.ready.load(std::memory_order_acquire);
                    size_t limit=0;
                    if(sealed)limit=b.used;
                    else {
                        // Age-limited background drain of an open buffer's committed
                        // prefix: never publish a prefix younger than drain_age_us.
                        if(!c.drain_age_us)continue;
                        limit=b.committed.load(std::memory_order_acquire);
                        if(limit<=drained[id]||b.token!=emitting)continue;
                        uint16_t first=0;std::memcpy(&first,b.bytes.data()+drained[id],2);
                        if(now<record_ticks(b.bytes.data()+drained[id]+2,first,g)+c.drain_age_us*1000)continue;
                        ++partial_drains;
                    }
                    if(b.token!=emitting)continue;
                    progress=true;
                    if(!started[id]) {
                        started[id]=1;
                        discarding[id]=c.drop_buffer_every && drained_buffers++%c.drop_buffer_every==0;
                    }
                    const bool discard=discarding[id];
                    if(!discard&&sealed)snapshot(g,id,b.referenced.data(),false);
                    else if(!discard) {
                        prefix_referenced.fill(false);
                        for(size_t pos=drained[id];pos<limit;) {
                            uint16_t length=0;std::memcpy(&length,b.bytes.data()+pos,2);pos+=2;
                            if(length>128||pos+length>limit)throw std::runtime_error("prefix length");
                            auto key=record_key(b.bytes.data()+pos,length,id);
                            if(key>c.capacity)throw std::runtime_error("prefix key");
                            if(key)prefix_referenced[key-1]=true;
                            pos+=length;
                        }
                        snapshot(g,id,prefix_referenced.data(),false);
                    }
                    for(size_t pos=drained[id];pos<limit;) {
                        uint16_t length=0;std::memcpy(&length,b.bytes.data()+pos,2);pos+=2;
                        if(length>128||pos+length>limit)throw std::runtime_error("buffer length");
                        if(discard) {++discarded_events;pos+=length;continue;}
                        auto* wire=b.bytes.data()+pos;size_t n=length;
                        if(c.mode=="staged") {
                            if(length<sizeof(Raw))throw std::runtime_error("raw length");
                            Raw raw{};std::memcpy(&raw,wire,sizeof(raw));Value value{};
                            if(!raw.key) {
                                if(length!=sizeof(Raw)+VALUE_BYTES)throw std::runtime_error("raw inline length");
                                std::memcpy(value.data(),wire+sizeof(Raw),VALUE_BYTES);
                            }
                            n=event_frame(output.data(),raw.ticks,g.anchor,raw.sequence,id,raw.key,value);wire=output.data();
                        }
                        // Parse references to emit definitions even without
                        // verification. This is a prototype background cost,
                        // not required by the proposed production writer.
                        const auto* p=wire;auto* end=wire+n;
                        if(get(p,end)!=n||get(p,end)!=EVENT_TYPE)throw std::runtime_error("frame");
                        auto z=get(p,end);if(z&1)++negative_offsets;
                        auto seq=get(p,end);if(get(p,end)!=id||p==end)throw std::runtime_error("producer");
                        auto tag=*p++;uint32_t key=0;
                        const Value* value=nullptr;
                        if(tag==2) {
                            auto k=get(p,end);if(k<=id*POOL_CAP||k>id*POOL_CAP+c.capacity)throw std::runtime_error("pool key");key=uint32_t(k-id*POOL_CAP);
                            value=&g.pools[id].entries[key-1].value;
                            if(!g.emitted[id][key-1])throw std::runtime_error("unpublished definition");
                            if(p!=end)throw std::runtime_error("pool trailing");
                        }else if(tag==3) {
                            if(get(p,end)!=VALUE_BYTES||end-p!=VALUE_BYTES)throw std::runtime_error("inline");
                        }else throw std::runtime_error("string tag");
                        if(c.verify) {
                            if(seq>=c.events||(previous[id]!=NO_GENERATION&&seq<=previous[id]))throw std::runtime_error("order");
                            previous[id]=seq;
                            if(std::memcmp(value?value->data():p,values[seq%c.cardinality].data(),VALUE_BYTES))throw std::runtime_error("value");
                            if(seq%17==0&&z!=1)throw std::runtime_error("negative timestamp");
                            if(key&&!g.emitted[id][key-1])throw std::runtime_error("missing definition");
                        }
                        sink.write(wire,n);g.body_bytes+=n;++g.accepted;++sink.events;pos+=length;
                    }
                    drained[id]=limit;
                    publish(g);
                    {
                        auto published=now_ns();
                        auto count=b.capture_count.load(std::memory_order_acquire);
                        auto& i=sample_index[id];
                        for(;i<count&&b.captures[i].end<=limit;++i) {
                            if(discard){++discarded_capture_samples;continue;}
                            if(b.captures[i].ticks>published)throw std::runtime_error("publication before capture");
                            publication_latency.push_back(published-b.captures[i].ticks);
                        }
                        if(sealed&&i!=count)throw std::runtime_error("unpublished capture samples");
                    }
                    if(sealed) {
                        drained[id]=sample_index[id]=0;started[id]=discarding[id]=0;
                        b.committed.store(0,std::memory_order_relaxed);
                        b.ready.store(false,std::memory_order_release);read_index[id]=(read_index[id]+1)%BUFFER_COUNT;
                    }
                }
                for(size_t id=0;id<lanes.size()&&!stalled;++id) {
                    auto& b=lanes[id]->buffers[lane_read[id]];
                    const bool sealed=b.ready.load(std::memory_order_acquire);
                    size_t limit=0;
                    if(sealed)limit=b.used;
                    else {
                        if(!c.drain_age_us)continue;
                        limit=b.committed.load(std::memory_order_acquire);
                        if(limit<=lane_drained[id]||b.token!=emitting)continue;
                        uint16_t first=0;std::memcpy(&first,b.bytes.get()+lane_drained[id],2);
                        if(now<lane_ticks(b.bytes.get()+lane_drained[id]+2,first,g)+c.drain_age_us*1000)continue;
                        ++lane_partial_drains;
                    }
                    if(b.token!=emitting)continue;
                    progress=true;
                    const auto drain_start=now_ns();
                    drain_lane(g,id,b,lane_drained[id],limit);
                    publish(g);
                    sample_drain_ns+=now_ns()-drain_start;
                    lane_drained[id]=limit;
                    if(sealed) {
                        lane_drained[id]=0;b.committed.store(0,std::memory_order_relaxed);
                        b.ready.store(false,std::memory_order_release);lane_read[id]=(lane_read[id]+1)%LANE_BUFFERS;
                    }
                }
                if(emitting!=active.load()&&!pinned(emitting)) {
                    close(g);++emitting;metadata(*generations[emitting%2]);progress=true;
                }
                if(all_done&&!pinned(emitting)&&emitting==active.load()) {
                    if(defer_start)max_defer_ns=std::max(max_defer_ns,now-defer_start);
                    close(*generations[emitting%2],true);break;}
                if(!progress)std::this_thread::sleep_for(std::chrono::microseconds(20));
            }
        }catch(...){consumer_error=std::current_exception();}
        measured_worker=false;
    });
    std::thread sampler;
    if(signals)sampler=std::thread([&] {
        // Thread-directed SIGPROF at signal_hz per producer. Standard signals
        // coalesce while pending, so delivered handler entries can be fewer.
        while(!go.load(std::memory_order_acquire))std::this_thread::yield();
        const auto period=uint64_t(1e9/double(c.signal_hz));auto next=start;
        while(producers_finished.load()<c.producers) {
            for(auto& t:threads){pthread_kill(t.native_handle(),SIGPROF);++signals_sent;}
            next+=period;std::this_thread::sleep_until(Clock::time_point(std::chrono::nanoseconds(next)));
        }
        sampler_stopped.store(true,std::memory_order_release);
    });
    while(ready.load()!=c.producers+1)std::this_thread::yield();
    auto cpu_start=cpu_seconds();start=now_ns();generations[0]->anchor=start;go.store(true,std::memory_order_release);
    if(sampler.joinable())sampler.join();
    for(auto& t:threads)t.join();consumer.join();
    auto seconds=double(now_ns()-start)/1e9;auto cpu=cpu_seconds()-cpu_start;
    if(consumer_error)std::rethrow_exception(consumer_error);
    uint64_t accepted=0,dropped=0,hits=0,misses=0,fallbacks=0,probes=0,handoffs=0,intermediate=0,queue_drops=0,acquire_drops=0;
    uint64_t periodic_flushes=0,capture_samples=0;
    std::vector<uint64_t> success,drop,hit,miss,handoff;
    for(auto& p:producers) {
        periodic_flushes+=p->periodic_flushes;capture_samples+=p->capture_samples;
        accepted+=p->accepted;dropped+=p->dropped;hits+=p->hits;misses+=p->misses;fallbacks+=p->fallbacks;
        probes+=p->probes;handoffs+=p->handoffs;intermediate+=p->intermediate;queue_drops+=p->queue_drops;acquire_drops+=p->acquire_drops;
        for(auto pair:{std::make_pair(&success,&p->success),std::make_pair(&drop,&p->drop),std::make_pair(&hit,&p->hit_latency),std::make_pair(&miss,&p->miss_latency),std::make_pair(&handoff,&p->handoff_latency)})
            pair.first->insert(pair.first->end(),pair.second->begin(),pair.second->end());
    }
    if(publication_latency.size()+discarded_capture_samples!=capture_samples)throw std::runtime_error("capture sample accounting");
    if(sink.events+discarded_events!=accepted||accepted+dropped!=c.events*c.producers||dropped!=queue_drops+acquire_drops)throw std::runtime_error("accounting");
    uint64_t lane_entered=0,lane_accepted=0,lane_queue=0,lane_acquire=0,lane_pool=0,lane_nested=0,lane_inactive=0,lane_interrupted=0,lane_intermediate=0,lane_handoffs=0;
    uint64_t lane_cas_retries=0,lane_wasted=0,lane_hits=0,lane_misses=0,lane_raw=0,nested_entries=0,nested_samples=0;
    std::vector<uint64_t> handler;
    for(auto& l:lanes) {
        lane_entered+=l->entered;lane_accepted+=l->accepted;lane_queue+=l->queue_drops;lane_acquire+=l->acquire_drops;
        lane_pool+=l->pool_drops;lane_nested+=l->nested.load();lane_inactive+=l->inactive.load();lane_interrupted+=l->interrupted;
        lane_intermediate+=l->intermediate;lane_handoffs+=l->handoffs;lane_cas_retries+=l->cas_retries;lane_wasted+=l->wasted_entries;
        lane_hits+=l->hits;lane_misses+=l->misses;lane_raw+=l->raw_records;
        if(l->level){nested_entries+=l->entered;nested_samples+=l->accepted;}
        handler.insert(handler.end(),l->handler_ns.begin(),l->handler_ns.begin()+long(l->handler_count));
    }
    if(lane_entered!=lane_accepted+lane_queue+lane_acquire+lane_pool||samples_committed+stage_pool_drops!=lane_accepted)
        throw std::runtime_error("sample accounting");
    // Lane storage excludes the per-lane handler-timing array (harness).
    const auto lane_storage_bytes=lanes.size()*(sizeof(Lane)-sizeof(std::array<uint32_t,HANDLER_SAMPLES>)+LANE_BUFFERS*lane_bytes);
    const auto table_storage_bytes=signals?2*(sizeof(StackTable)+c.stack_capacity*(sizeof(StackRef)+2*sizeof(uint32_t)+1)+
                                               c.frame_capacity*sizeof(StackFrame)):0;
    auto recorder_bytes=2*(sizeof(Generation)+c.producers*(sizeof(Pool)+sizeof(std::array<bool,POOL_CAP>)+sizeof(std::array<uint32_t,POOL_CAP>)))+
                        c.producers*(sizeof(Producer)+c.cardinality*sizeof(Cache));
    const auto visibility_buffer_bytes=c.producers*BUFFER_COUNT*sizeof(std::array<Capture,CAPTURE_SAMPLES>);
    recorder_bytes-=visibility_buffer_bytes;
    std::cout<<"{\"mode\":\""<<c.mode<<"\",\"workload\":\""<<c.workload<<"\",\"verify\":"<<(c.verify?"true":"false")
        <<",\"producers\":"<<c.producers<<",\"events\":"<<c.events<<",\"cardinality\":"<<c.cardinality<<",\"capacity\":"<<c.capacity
        <<",\"flush_us\":"<<c.flush_us<<",\"flush_clock\":"<<c.flush_clock<<",\"burst\":"<<c.burst
        <<",\"streaming\":"<<(c.streaming?"true":"false")<<",\"finalize_on_close\":"<<(c.finalize_on_close?"true":"false")
        <<",\"finalized_chunks\":"<<sink.finalized<<",\"finalize_skipped\":"<<sink.finalize_skipped<<",\"observe_publication\":"<<(c.observe_publication?"true":"false")<<",\"visibility_every\":"<<c.visibility_every
        <<",\"drain_age_us\":"<<c.drain_age_us<<",\"partial_drains\":"<<partial_drains
        <<",\"periodic_flushes\":"<<periodic_flushes<<",\"publication_samples\":"<<publication_latency.size()
        <<",\"publication_discarded_samples\":"<<discarded_capture_samples
        <<",\"publication_p50_ns\":"<<percentile(publication_latency,.5)
        <<",\"publication_p99_ns\":"<<percentile(publication_latency,.99)
        <<",\"publication_p999_ns\":"<<percentile(publication_latency,.999)
        <<",\"publication_max_ns\":"<<percentile(publication_latency,1)
        <<",\"visibility_buffer_storage_bytes\":"<<visibility_buffer_bytes
        <<",\"rate\":"<<c.rate<<",\"rotate_us\":"<<c.rotate_us<<",\"stall_ms\":"<<c.stall_ms<<",\"pause_ms\":"<<c.pause_ms
        <<",\"committed\":"<<sink.events<<",\"discarded_after_acceptance\":"<<discarded_events
        <<",\"post_pause_accepted\":"<<producers[0]->post_pause_accepted
        <<",\"accepted\":"<<accepted<<",\"dropped\":"<<dropped<<",\"queue_drops\":"<<queue_drops<<",\"acquire_drops\":"<<acquire_drops
        <<",\"hits\":"<<hits<<",\"misses\":"<<misses<<",\"inline_fallbacks\":"<<fallbacks<<",\"probes\":"<<probes
        <<",\"handoffs\":"<<handoffs<<",\"rotations\":"<<rotations<<",\"rotation_deferred_polls\":"<<rotation_deferred
        <<",\"max_rotation_defer_ns\":"<<max_defer_ns
        <<",\"max_live_generations\":"<<max_live<<",\"max_queued_buffers\":"<<max_queued
        <<",\"recorder_storage_bytes\":"<<recorder_bytes<<",\"sample_reserved_bytes\":"<<c.producers*c.events*(5+(c.observe_publication?1:0))*sizeof(uint64_t)
        <<",\"native_chunk_output\":true,\"header_publications\":"<<sink.publications
        <<",\"wire_bytes\":"<<sink.wire_bytes<<",\"definitions\":"<<sink.definitions<<",\"chunks\":"<<sink.chunks
        <<",\"wire_bytes_per_accepted\":"<<(accepted?double(sink.wire_bytes)/accepted:0)
        <<",\"intermediate_bytes_per_accepted\":"<<(accepted?double(intermediate)/accepted:0)
        <<",\"wall_seconds\":"<<seconds<<",\"cpu_seconds\":"<<cpu
        <<",\"cpu_ns_per_attempt\":"<<cpu*1e9/(accepted+dropped)
        <<",\"cpu_ns_per_committed\":"<<(sink.events?cpu*1e9/sink.events:0)
        <<",\"cpu_ns_per_accepted\":"<<(accepted?cpu*1e9/accepted:0)
        <<",\"accepted_per_second\":"<<accepted/seconds
        <<",\"producer_p50_ns\":"<<percentile(success,.5)<<",\"producer_p99_ns\":"<<percentile(success,.99)
        <<",\"producer_p999_ns\":"<<percentile(success,.999)<<",\"hit_p99_ns\":"<<percentile(hit,.99)
        <<",\"miss_p99_ns\":"<<percentile(miss,.99)<<",\"handoff_p99_ns\":"<<percentile(handoff,.99)
        <<",\"drop_p99_ns\":"<<percentile(drop,.99)<<",\"success_samples\":"<<success.size()
        <<",\"hit_samples\":"<<hit.size()<<",\"miss_samples\":"<<miss.size()<<",\"handoff_samples\":"<<handoff.size()
        <<",\"recording_new_calls\":"<<recording_allocations.load()<<",\"worker_new_calls\":"<<worker_allocations.load()
        <<",\"negative_offsets\":"<<negative_offsets
        <<",\"signal_hz\":"<<c.signal_hz<<",\"stack_mode\":\""<<c.stack_mode<<"\",\"stack_sharing\":\""<<c.stack_sharing<<"\""
        <<",\"stacks\":"<<c.stacks<<",\"stack_depth\":"<<c.stack_depth<<",\"nodefer\":"<<c.nodefer
        <<",\"signals_sent\":"<<signals_sent<<",\"handler_entries\":"<<lane_entered<<",\"samples_accepted\":"<<lane_accepted
        <<",\"sample_nested\":"<<lane_nested<<",\"sample_inactive\":"<<lane_inactive<<",\"sample_unregistered\":"<<sig_unregistered.load()
        <<",\"sample_queue_drops\":"<<lane_queue<<",\"sample_acquire_drops\":"<<lane_acquire<<",\"sample_pool_drops\":"<<lane_pool
        <<",\"stack_cas_retries\":"<<lane_cas_retries<<",\"stack_wasted_entries\":"<<lane_wasted
        <<",\"stage_pool_drops\":"<<stage_pool_drops<<",\"samples_committed\":"<<samples_committed
        <<",\"interrupted_recording\":"<<lane_interrupted<<",\"lane_handoffs\":"<<lane_handoffs<<",\"lane_partial_drains\":"<<lane_partial_drains
        <<",\"sample_event_bytes\":"<<sample_event_bytes<<",\"sample_definition_bytes\":"<<sample_definition_bytes
        <<",\"stack_definitions\":"<<stack_definitions<<",\"method_definitions\":"<<method_definitions
        <<",\"sample_bytes_per_committed\":"<<(samples_committed?double(sample_event_bytes+sample_definition_bytes)/samples_committed:0)
        <<",\"lane_intermediate_bytes_per_accepted\":"<<(lane_accepted?double(lane_intermediate)/lane_accepted:0)
        <<",\"sample_drain_ns_per_committed\":"<<(samples_committed?double(sample_drain_ns)/samples_committed:0)
        <<",\"handler_samples\":"<<handler.size()<<",\"handler_p50_ns\":"<<percentile(handler,.5)
        <<",\"handler_p99_ns\":"<<percentile(handler,.99)<<",\"handler_p999_ns\":"<<percentile(handler,.999)
        <<",\"handler_max_ns\":"<<percentile(handler,1)<<",\"lane_storage_bytes\":"<<lane_storage_bytes
        <<",\"table_storage_bytes\":"<<table_storage_bytes<<",\"lane_kib\":"<<c.lane_kib
        <<",\"stack_capacity\":"<<c.stack_capacity<<",\"frame_capacity\":"<<c.frame_capacity
        <<",\"nest_levels\":"<<c.nest_levels<<",\"inject_every\":"<<c.inject_every
        <<",\"injected_points\":["<<sig_injected[0].load()<<","<<sig_injected[1].load()<<","<<sig_injected[2].load()<<","<<sig_injected[3].load()<<"]"
        <<",\"nested_entries\":"<<nested_entries<<",\"nested_samples\":"<<nested_samples
        <<",\"hybrid_hits\":"<<lane_hits<<",\"hybrid_misses\":"<<lane_misses<<",\"raw_records\":"<<lane_raw
        <<",\"checksum\":"<<sink.checksum<<"}\n";
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}
