package main

import (
	"context"
	"os"
	"runtime"
	"runtime/trace"
	"sync"
)

func work(wg *sync.WaitGroup, ping <-chan struct{}, pong chan<- struct{}) {
	defer wg.Done()
	ctx, task := trace.NewTask(context.Background(), "tcf-task")
	defer task.End()
	trace.StartRegion(ctx, "inner").End()
	trace.Logf(ctx, "stage", "mid")
	_ = make([]byte, 1<<20)
	runtime.GC()
	f, _ := os.Open("/etc/hosts")
	_ = f.Close()
	for i := 0; i < 10; i++ {
		<-ping
		pong <- struct{}{}
	}
}

func pinger(ping chan<- struct{}, pong <-chan struct{}, n int) {
	for i := 0; i < 10*n; i++ {
		ping <- struct{}{}
		<-pong
	}
}

func main() {
	f, err := os.Create("/tmp/tcf-gotrace.out")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	trace.Start(f)
	var wg sync.WaitGroup
	ping := make(chan struct{})
	pong := make(chan struct{})
	go pinger(ping, pong, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { work(&wg, ping, pong) }()
	}
	wg.Wait()
	trace.Stop()
}
