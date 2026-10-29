package core

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Arceliar/phony"
	"github.com/gologme/log"
)

// Count only fixed NodeInfo events; never retain keys or payloads from logs.
type nodeInfoTestLog struct {
	mu       sync.Mutex
	writes   int
	failed   int
	readDone <-chan struct{}
}

func (l *nodeInfoTestLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if bytes.Contains(p, []byte("NodeInfo request write started")) {
		l.writes++
	}
	if bytes.Contains(p, []byte("NodeInfo request write failed")) {
		l.failed++
	}
	return len(p), nil
}

func (l *nodeInfoTestLog) counts() (writes, failed int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.writes, l.failed
}

func nodeInfoRetryFixture(t *testing.T) (*Core, *nodeInfoTestLog, func()) {
	t.Helper()
	t.Log("NodeInfo retry fixture: entry")
	defer t.Log("NodeInfo retry fixture: exit")
	stats := new(nodeInfoTestLog)
	logger := log.New(stats, "", 0)
	logger.EnableLevel("debug")
	c, stop, readDone := newNodeInfoCore(t, logger)
	stats.readDone = readDone
	return c, stats, stop
}

func TestNodeInfoRetryTimeoutCleanup(t *testing.T) {
	t.Log("NodeInfo retry timeout: entry")
	defer t.Log("NodeInfo retry timeout: exit")
	c, stats, _ := nodeInfoRetryFixture(t)
	_, err := c.proto.nodeinfo.requestNodeInfo(keyArray{1}, 180*time.Millisecond, 40*time.Millisecond)
	if err == nil || err.Error() != "timed out waiting for response" {
		t.Fatal("expected timeout")
	}
	pending := 0
	phony.Block(&c.proto.nodeinfo, func() { pending = len(c.proto.nodeinfo.callbacks) })
	writes, _ := stats.counts()
	if writes < 2 || writes > 5 || pending != 0 {
		t.Fatalf("writes=%d pending=%d", writes, pending)
	}
	time.Sleep(100 * time.Millisecond)
	if after, _ := stats.counts(); after != writes {
		t.Fatal("writes continued after timeout")
	}
}

func TestNodeInfoResponseStopsRetries(t *testing.T) {
	t.Log("NodeInfo response: entry")
	defer t.Log("NodeInfo response: exit")
	c, stats, _ := nodeInfoRetryFixture(t)
	key := keyArray{2}
	finished := make(chan bool, 1)
	go func() {
		info, err := c.proto.nodeinfo.requestNodeInfo(key, time.Second, 100*time.Millisecond)
		finished <- err == nil && bytes.Equal(info, []byte(`{"fixture":"reply"}`))
	}()
	waitNodeInfoPending(t, c)
	c.proto.nodeinfo.handleRes(nil, key, json.RawMessage(`{"fixture":"reply"}`))
	c.proto.nodeinfo.handleRes(nil, key, json.RawMessage(`{"fixture":"duplicate"}`))
	select {
	case ok := <-finished:
		if !ok {
			t.Fatal("response did not complete request")
		}
	case <-time.After(time.Second):
		t.Fatal("response callback blocked")
	}
	phony.Block(&c.proto.nodeinfo, func() {})
	writes, _ := stats.counts()
	time.Sleep(220 * time.Millisecond)
	if after, _ := stats.counts(); after != writes || writes != 1 {
		t.Fatalf("writes before/after response=%d/%d, want 1/1", writes, after)
	}
}

func TestNodeInfoRetryShutdown(t *testing.T) {
	t.Log("NodeInfo shutdown: entry")
	defer t.Log("NodeInfo shutdown: exit")
	c, stats, stop := nodeInfoRetryFixture(t)
	finished := make(chan error, 1)
	go func() {
		_, err := c.proto.nodeinfo.requestNodeInfo(keyArray{3}, time.Second, 40*time.Millisecond)
		finished <- err
	}()
	waitNodeInfoPending(t, c)
	stop()
	select {
	case err := <-finished:
		if err == nil || err.Error() != "node is stopping" {
			t.Fatal("request did not stop with the core")
		}
	case <-time.After(time.Second):
		t.Fatal("request remained blocked after shutdown")
	}
	phony.Block(&c.proto.nodeinfo, func() {})
	writes, _ := stats.counts()
	time.Sleep(100 * time.Millisecond)
	if after, _ := stats.counts(); after != writes {
		t.Fatal("retry continued after shutdown")
	}
}

func TestNodeInfoQueuedAttemptCancelled(t *testing.T) {
	t.Log("NodeInfo queued attempt: entry")
	defer t.Log("NodeInfo queued attempt: exit")
	c, stats, _ := nodeInfoRetryFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	c.proto.nodeinfo.Act(nil, func() { close(entered); <-release })
	<-entered
	finished := make(chan error, 1)
	go func() {
		_, err := c.proto.nodeinfo.requestNodeInfo(keyArray{4}, 50*time.Millisecond, 10*time.Millisecond)
		finished <- err
	}()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("expected timeout while actor was blocked")
		}
	case <-time.After(time.Second):
		t.Fatal("handler timeout depended on the blocked actor")
	}
	unblock()
	pending := 0
	phony.Block(&c.proto.nodeinfo, func() { pending = len(c.proto.nodeinfo.callbacks) })
	if writes, _ := stats.counts(); writes != 0 || pending != 0 {
		t.Fatalf("cancelled queued writes=%d pending=%d", writes, pending)
	}
}

func TestNodeInfoOldRequestPreservesReplacement(t *testing.T) {
	t.Log("NodeInfo replacement ownership: entry")
	defer t.Log("NodeInfo replacement ownership: exit")
	c, _, _ := nodeInfoRetryFixture(t)
	key := keyArray{5}
	oldDone, newDone := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := c.proto.nodeinfo.requestNodeInfo(key, 150*time.Millisecond, 30*time.Millisecond)
		oldDone <- err
	}()
	waitNodeInfoPending(t, c)
	var old *nodeinfoCallback
	phony.Block(&c.proto.nodeinfo, func() { old = c.proto.nodeinfo.callbacks[key] })
	go func() {
		_, err := c.proto.nodeinfo.requestNodeInfo(key, time.Second, 100*time.Millisecond)
		newDone <- err
	}()
	until := time.Now().Add(time.Second)
	for {
		var current *nodeinfoCallback
		phony.Block(&c.proto.nodeinfo, func() { current = c.proto.nodeinfo.callbacks[key] })
		if current != nil && current != old {
			break
		}
		if time.Now().After(until) {
			t.Fatal("replacement request was not registered")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-oldDone:
		if err == nil {
			t.Fatal("old request unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("old request did not time out")
	}
	var current *nodeinfoCallback
	phony.Block(&c.proto.nodeinfo, func() { current = c.proto.nodeinfo.callbacks[key] })
	if current == nil || current == old {
		t.Fatal("old cleanup removed the replacement callback")
	}
	c.proto.nodeinfo.handleRes(nil, key, json.RawMessage(`{}`))
	select {
	case err := <-newDone:
		if err != nil {
			t.Fatal("replacement could not receive its response")
		}
	case <-time.After(time.Second):
		t.Fatal("replacement callback blocked")
	}
}

func TestNodeInfoFailedWriteIsBounded(t *testing.T) {
	t.Log("NodeInfo failed write: entry")
	defer t.Log("NodeInfo failed write: exit")
	c, stats, _ := nodeInfoRetryFixture(t)
	if err := c.Close(); err != nil {
		t.Fatal("packet connection close failed")
	}
	// Ironwood's encrypted writer observes closure through its reader loop.
	// Wait for that loop instead of racing the first write against shutdown.
	select {
	case <-stats.readDone:
	case <-time.After(time.Second):
		t.Fatal("closed connection reader did not stop")
	}
	_, err := c.proto.nodeinfo.requestNodeInfo(keyArray{6}, 100*time.Millisecond, 30*time.Millisecond)
	// The request deadline does not wait for an already running actor write.
	phony.Block(&c.proto.nodeinfo, func() {})
	writes, failed := stats.counts()
	if err == nil || writes < 1 || writes > 4 || failed != writes {
		t.Fatalf("failed writes=%d/%d, timeout=%t", failed, writes, err != nil)
	}
}

func TestNodeInfoAdminLocalAndInvalid(t *testing.T) {
	t.Log("NodeInfo local/invalid admin: entry")
	defer t.Log("NodeInfo local/invalid admin: exit")
	c, stats, _ := nodeInfoRetryFixture(t)
	for _, input := range []string{`{}`, `{"key":"` + hex.EncodeToString(c.PublicKey()) + `"}`} {
		if _, err := c.proto.nodeinfo.nodeInfoAdminHandler(json.RawMessage(input)); err != nil {
			t.Fatal("local NodeInfo failed")
		}
	}
	for _, input := range []string{`{`, `{"key":"zz"}`, `{"key":"00"}`} {
		if _, err := c.proto.nodeinfo.nodeInfoAdminHandler(json.RawMessage(input)); err == nil {
			t.Fatal("invalid input was accepted")
		}
	}
	if writes, _ := stats.counts(); writes != 0 {
		t.Fatal("local or invalid request sent remote traffic")
	}
}
