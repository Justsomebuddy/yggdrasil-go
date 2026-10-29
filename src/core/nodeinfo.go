package core

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"runtime"
	"time"

	iwt "github.com/Arceliar/ironwood/types"
	"github.com/Arceliar/phony"
	"github.com/yggdrasil-network/yggdrasil-go/src/version"
)

type nodeinfo struct {
	phony.Inbox
	proto      *protoHandler
	myNodeInfo json.RawMessage
	callbacks  map[keyArray]*nodeinfoCallback
}

type nodeinfoCallback struct {
	call    func(nodeinfo json.RawMessage)
	created time.Time
}

// Initialises the nodeinfo cache/callback maps, and starts a goroutine to keep
// the cache/callback maps clean of stale entries
func (m *nodeinfo) init(proto *protoHandler) {
	m.Act(nil, func() {
		m._init(proto)
	})
}

func (m *nodeinfo) _init(proto *protoHandler) {
	m.proto = proto
	m.callbacks = make(map[keyArray]*nodeinfoCallback)
	m._cleanup()
}

func (m *nodeinfo) _cleanup() {
	for boxPubKey, callback := range m.callbacks {
		if time.Since(callback.created) > time.Minute {
			delete(m.callbacks, boxPubKey)
		}
	}
	time.AfterFunc(time.Second*30, func() {
		m.Act(nil, m._cleanup)
	})
}

// Handles the callback, if there is one
func (m *nodeinfo) _callback(sender keyArray, nodeinfo json.RawMessage) {
	if callback, ok := m.callbacks[sender]; ok {
		callback.call(nodeinfo)
		delete(m.callbacks, sender)
	}
}

func (m *nodeinfo) _getNodeInfo() json.RawMessage {
	return m.myNodeInfo
}

// Set the current node's nodeinfo
func (m *nodeinfo) setNodeInfo(given map[string]any, privacy bool) (err error) {
	phony.Block(m, func() {
		err = m._setNodeInfo(given, privacy)
	})
	return
}

func (m *nodeinfo) _setNodeInfo(given map[string]any, privacy bool) error {
	newnodeinfo := make(map[string]any, len(given))
	maps.Copy(newnodeinfo, given)
	if !privacy {
		newnodeinfo["buildname"] = version.BuildName()
		newnodeinfo["buildversion"] = version.BuildVersion()
		newnodeinfo["buildplatform"] = runtime.GOOS
		newnodeinfo["buildarch"] = runtime.GOARCH
	}
	newjson, err := json.Marshal(newnodeinfo)
	switch {
	case err != nil:
		return fmt.Errorf("NodeInfo marshalling failed: %w", err)
	case len(newjson) > 16384:
		return fmt.Errorf("NodeInfo exceeds max length of 16384 bytes")
	default:
		m.myNodeInfo = newjson
		return nil
	}
}

func (m *nodeinfo) _sendReq(key keyArray) {
	m.proto.core.log.Debugln("NodeInfo request write started")
	defer m.proto.core.log.Debugln("NodeInfo request write finished")
	if _, err := m.proto.core.PacketConn.WriteTo([]byte{typeSessionProto, typeProtoNodeInfoRequest}, iwt.Addr(key[:])); err != nil {
		m.proto.core.log.Debugln("NodeInfo request write failed")
	}
}

// requestNodeInfo is called outside the actor. All attempts share one deadline
// and callback: a retry must not replace another request's callback or extend
// the six-second admin timeout. The durations are arguments for focused tests.
func (m *nodeinfo) requestNodeInfo(key keyArray, timeout, retryInterval time.Duration) (info json.RawMessage, err error) {
	m.proto.core.log.Debugln("NodeInfo remote request started")
	defer func() {
		m.proto.core.log.Debugf("NodeInfo remote request finished (success=%t)", err == nil)
	}()
	deadline := time.Now().Add(timeout)
	ch := make(chan json.RawMessage, 1)
	done := make(chan struct{})
	request := &nodeinfoCallback{
		created: time.Now(),
		call: func(info json.RawMessage) {
			select {
			case ch <- info:
			default:
			}
		},
	}
	send := func(initial bool) {
		m.Act(nil, func() {
			m.proto.core.log.Debugln("NodeInfo request dispatch started")
			defer m.proto.core.log.Debugln("NodeInfo request dispatch finished")
			select {
			case <-done:
				m.proto.core.log.Debugln("NodeInfo request dispatch cancelled")
				return
			case <-m.proto.core.ctx.Done():
				m.proto.core.log.Debugln("NodeInfo request dispatch stopped")
				return
			default:
			}
			if !time.Now().Before(deadline) {
				m.proto.core.log.Debugln("NodeInfo request dispatch expired")
				return
			}
			if initial {
				m.callbacks[key] = request
			} else if m.callbacks[key] != request {
				m.proto.core.log.Debugln("NodeInfo retry no longer pending")
				return
			} else {
				m.proto.core.log.Debugln("NodeInfo request retry")
			}
			m._sendReq(key)
		})
	}
	defer func() {
		close(done)
		m.Act(nil, func() {
			if m.callbacks[key] == request {
				delete(m.callbacks, key)
				m.proto.core.log.Debugln("NodeInfo pending request removed")
			}
		})
	}()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	retry := time.NewTicker(retryInterval)
	defer retry.Stop()
	send(true)
	for {
		select {
		case <-m.proto.core.ctx.Done():
			return nil, errors.New("node is stopping")
		case <-timer.C:
			return nil, errors.New("timed out waiting for response")
		case <-retry.C:
			send(false)
		case info := <-ch:
			return info, nil
		}
	}
}

func (m *nodeinfo) handleReq(from phony.Actor, key keyArray) {
	m.Act(from, func() {
		m._sendRes(key)
	})
}

func (m *nodeinfo) handleRes(from phony.Actor, key keyArray, info json.RawMessage) {
	m.Act(from, func() {
		m._callback(key, info)
	})
}

func (m *nodeinfo) _sendRes(key keyArray) {
	bs := append([]byte{typeSessionProto, typeProtoNodeInfoResponse}, m._getNodeInfo()...)
	_, _ = m.proto.core.PacketConn.WriteTo(bs, iwt.Addr(key[:]))
}

// Admin socket stuff

type GetNodeInfoRequest struct {
	Key string `json:"key"`
}
type GetNodeInfoResponse map[string]json.RawMessage

func (m *nodeinfo) nodeInfoAdminHandler(in json.RawMessage) (out any, err error) {
	m.proto.core.log.Debugln("NodeInfo admin request started")
	defer func() {
		m.proto.core.log.Debugf("NodeInfo admin request finished (success=%t)", err == nil)
	}()
	var req GetNodeInfoRequest
	if err := json.Unmarshal(in, &req); err != nil {
		return nil, err
	}
	if self := hex.EncodeToString(m.proto.core.PublicKey()); req.Key == self || req.Key == "" {
		return GetNodeInfoResponse{self: m.myNodeInfo}, nil
	}
	var key keyArray
	var kbs []byte
	if kbs, err = hex.DecodeString(req.Key); err != nil {
		return nil, fmt.Errorf("failed to decode public key: %w", err)
	}
	if len(kbs) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid public key length")
	}
	copy(key[:], kbs)
	info, err := m.requestNodeInfo(key, 6*time.Second, 2*time.Second)
	if err != nil {
		return nil, err
	}
	var msg json.RawMessage
	if err := msg.UnmarshalJSON(info); err != nil {
		return nil, err
	}
	res := GetNodeInfoResponse{hex.EncodeToString(kbs): msg}
	return res, nil
}
