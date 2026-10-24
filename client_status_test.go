package mqtt

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestStatusCallbacksPreserveOrderWithoutBlockingTransitions(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	delivered := make(chan ConnectionStatus, 128)
	first := true
	c := newShutdownTestClient(t, WithOnConnectionStatusHandler(func(c Client, s ConnectionStatus) {
		_ = c.Status() // Callback may inspect the client without deadlocking.
		if first {
			first = false
			close(entered)
			<-release
		}
		delivered <- s
	}))
	c.setConnectionStatus(Connecting, nil)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("callback did not start")
	}
	failure := errors.New("connection failed")
	expected := []ConnectionStatus{{Code: Connecting}}
	// Include stop/start boundaries and enough transitions to exceed a small
	// fixed-size channel while the first callback is blocked.
	for i := 0; i < 20; i++ {
		expected = append(expected, ConnectionStatus{Code: Connected}, ConnectionStatus{Code: Disconnected, Err: failure}, ConnectionStatus{Code: Stopped}, ConnectionStatus{Code: Connecting})
	}
	queued := make(chan struct{})
	go func() {
		for _, s := range expected[1:] {
			c.setConnectionStatus(s.Code, s.Err)
		}
		close(queued)
	}()
	select {
	case <-queued:
	case <-time.After(time.Second):
		t.Fatal("blocked callback stalled transitions")
	}
	select {
	case s := <-delivered:
		t.Fatalf("callback overtook first transition: %+v", s)
	default:
	}
	unblock()
	for _, want := range expected {
		select {
		case got := <-delivered:
			if got != want {
				t.Fatalf("got %+v, want %+v", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("missing callback")
		}
	}
	// Verify the idle worker exits and can be restarted for another transition.
	deadline := time.Now().Add(time.Second)
	for {
		c.statusMutex.Lock()
		idle := !c.statusDispatching && c.statusQueue == nil
		c.statusMutex.Unlock()
		if idle {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("dispatcher did not exit")
		}
		time.Sleep(time.Millisecond)
	}
	c.setConnectionStatus(Stopped, nil)
	select {
	case got := <-delivered:
		if got.Code != Stopped {
			t.Fatalf("got %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not restart")
	}
}

func TestStatusConcurrentReads(t *testing.T) {
	c := newShutdownTestClient(t)
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for j := 0; j < 1000; j++ {
				_ = c.Status()
			}
		}()
	}
	for i := 0; i < 1000; i++ {
		c.setConnectionStatus(i%4, nil)
	}
	readers.Wait()
	if got := c.Status().Code; got != 999%4 {
		t.Fatalf("unexpected final status: %d", got)
	}
}
