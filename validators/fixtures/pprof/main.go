package main

import (
	"os"
	"runtime"
	"runtime/pprof"

	"context"
	"sync"
)

func fib(n int) int {
	if n < 2 {
		return n
	}
	return fib(n-1) + fib(n-2)
}

func hash(bytes []byte) uint64 {
	h := uint64(1469598103934665603)
	for _, c := range bytes {
		h = (h ^ uint64(c)) * 1099511628211
	}
	return h
}

func main() {
	f, err := os.Create("/tmp/tcf-pprof-cpu.pprof")
	if err != nil {
		panic(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			buf := make([]byte, 64)
			for i := 0; i < 26000000; i++ {
				buf[i%64] = byte(i)
				if i%500 == 0 {
					hash(buf)
				}
			}
		}(w)
	}
	// labeled CPU samples via pprof.Do (Go 1.26+ labeled sampling)
	ctx := context.Background()
	pprof.Do(ctx, pprof.Labels("worker", "hot", "phase", "spin"), func(c context.Context) {
		fib(30)
	})
	fib(28)
	pprof.StopCPUProfile()
	f.Close()

	// labeled heap profile: pprof.Do attaches labels to allocations? labels attach
	// to samples via profile writer for labeled goroutines' memory profile.
	hf, err := os.Create("/tmp/tcf-pprof-heap.pprof")
	if err != nil {
		panic(err)
	}
	pprof.Do(ctx, pprof.Labels("worker", "w1", "phase", "alloc"), func(c2 context.Context) {
		hold := make([][]byte, 0, 2000)
		for i := 0; i < 2000; i++ {
			hold = append(hold, make([]byte, 128+i))
			_ = hold[len(hold)-1]
		}
		runtime.GC() // force accounting flush
		if err := pprof.Lookup("allocs").WriteTo(hf, 0); err != nil {
			panic(err)
		}
		_ = hold
	})
	hf.Close()
}