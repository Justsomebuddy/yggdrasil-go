package core

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	iwn "github.com/Arceliar/ironwood/network"
	"github.com/Arceliar/phony"
)

func newNodeInfoCore(t *testing.T, logger Logger) (*Core, func(), <-chan struct{}) {
	t.Helper()
	t.Log("NodeInfo fixture: entry")
	defer t.Log("NodeInfo fixture: exit")
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal("fixture key generation failed")
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: hex.EncodeToString(public)},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal("fixture certificate generation failed")
	}
	cert := &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}
	c, err := New(cert, logger, NodeInfoPrivacy(true), NodeInfo{"fixture": "nodeinfo-test"})
	if err != nil {
		t.Fatal("core creation failed")
	}
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		buf := make([]byte, 65535)
		for {
			if _, _, err := c.ReadFrom(buf); err != nil {
				return
			}
		}
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			t.Log("NodeInfo fixture: cleanup")
			c.Stop()
			select {
			case <-readDone:
			case <-time.After(3 * time.Second):
				t.Error("protocol reader did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return c, stop, readDone
}

func waitNodeInfoPending(t *testing.T, c *Core) {
	t.Helper()
	t.Log("NodeInfo pending wait: entry")
	defer t.Log("NodeInfo pending wait: exit")
	until := time.Now().Add(time.Second)
	for {
		pending := 0
		phony.Block(&c.proto.nodeinfo, func() { pending = len(c.proto.nodeinfo.callbacks) })
		if pending != 0 {
			return
		}
		if time.Now().After(until) {
			t.Fatal("initial NodeInfo request was not registered")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNodeInfoColdStart(t *testing.T) {
	t.Log("NodeInfo cold start: entry")
	defer t.Log("NodeInfo cold start: exit")
	a, _, _ := newNodeInfoCore(t, nil)
	b, _, _ := newNodeInfoCore(t, nil)
	input, err := json.Marshal(GetNodeInfoRequest{Key: hex.EncodeToString(b.PublicKey())})
	if err != nil {
		t.Fatal("request encoding failed")
	}
	type result struct {
		value any
		err   error
	}
	completed := make(chan result, 1)
	started := time.Now()
	// Exactly one admin request, before any peer/path/session can exist.
	go func() {
		value, err := a.proto.nodeinfo.nodeInfoAdminHandler(input)
		completed <- result{value, err}
	}()
	waitNodeInfoPending(t, a)
	if len(a.GetPeers()) != 0 || len(a.GetPaths()) != 0 || len(a.GetSessions()) != 0 {
		t.Fatal("request was not cold")
	}
	listenURL, err := url.Parse("tcp://127.0.0.1:0")
	if err != nil {
		t.Fatal("listener URL failed")
	}
	listener, err := b.Listen(listenURL, "")
	if err != nil {
		t.Fatal("listener failed")
	}
	t.Cleanup(listener.Cancel)
	peerURL, err := url.Parse("tcp://" + listener.Addr().String())
	if err != nil {
		t.Fatal("peer URL failed")
	}
	if err := a.CallPeer(peerURL, ""); err != nil {
		t.Fatal("peer connection failed")
	}
	select {
	case got := <-completed:
		if got.err != nil {
			t.Fatal("the single cold NodeInfo admin request timed out")
		}
		response, ok := got.value.(GetNodeInfoResponse)
		if !ok {
			t.Fatal("wrong response type")
		}
		var info map[string]string
		if err := json.Unmarshal(response[hex.EncodeToString(b.PublicKey())], &info); err != nil || info["fixture"] != "nodeinfo-test" {
			t.Fatal("wrong remote NodeInfo")
		}
		if time.Since(started) >= 6*time.Second {
			t.Fatal("NodeInfo timeout budget was extended")
		}
	case <-time.After(7 * time.Second):
		t.Fatal("NodeInfo admin handler did not return")
	}
}

func TestNodeInfoUnreachableBudget(t *testing.T) {
	t.Log("NodeInfo unreachable budget: entry")
	defer t.Log("NodeInfo unreachable budget: exit")
	c, _, _ := newNodeInfoCore(t, nil)
	var dest keyArray
	dest[0] = c.PublicKey()[0] ^ 1
	var lookups atomic.Int32
	c.PacketConn.PacketConn.Debug.SetDebugLookupLogger(func(info iwn.DebugLookupInfo) {
		if string(info.Target) == string(dest[:]) {
			lookups.Add(1)
		}
	})
	input, err := json.Marshal(GetNodeInfoRequest{Key: hex.EncodeToString(dest[:])})
	if err != nil {
		t.Fatal("request encoding failed")
	}
	started := time.Now()
	_, err = c.proto.nodeinfo.nodeInfoAdminHandler(input)
	if err == nil || err.Error() != "timed out waiting for response" {
		t.Fatal("expected the original timeout error")
	}
	if elapsed := time.Since(started); elapsed < 5900*time.Millisecond || elapsed > 7*time.Second {
		t.Fatalf("timeout budget changed: %.3fs", elapsed.Seconds())
	}
	pending := 0
	phony.Block(&c.proto.nodeinfo, func() { pending = len(c.proto.nodeinfo.callbacks) })
	if n := lookups.Load(); n != 3 || pending != 0 {
		t.Fatalf("lookups=%d pending=%d, want 3 and 0", n, pending)
	}
}
