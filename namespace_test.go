package websocket

import (
	"context"
	"errors"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeConn is an in-memory WSConn for namespace tests.
type fakeConn struct {
	in     chan Message
	out    chan Message
	closed chan struct{}
	once   sync.Once

	mu          sync.Mutex
	closeCode   int
	closeReason string
	closeCount  int
	readLimit   int64
	pings       int

	ping  func(ctx context.Context) error // nil = pong immediately
	write func(v any) error               // nil = success
}

func newFakeConn() *fakeConn {
	return &fakeConn{
		in:     make(chan Message, 16),
		out:    make(chan Message, 1024),
		closed: make(chan struct{}),
	}
}

func (f *fakeConn) ReadJSON(v any) error {
	select {
	case m := <-f.in:
		*(v.(*Message)) = m
		return nil
	case <-f.closed:
		return errors.New("closed")
	}
}

func (f *fakeConn) WriteJSON(v any) error {
	if f.write != nil {
		return f.write(v)
	}
	select {
	case f.out <- v.(Message):
	default:
	}
	return nil
}

func (f *fakeConn) Close(code int, reason string) error {
	f.mu.Lock()
	f.closeCount++
	if f.closeCount == 1 {
		f.closeCode, f.closeReason = code, reason
	}
	f.mu.Unlock()
	f.once.Do(func() { close(f.closed) })
	return nil
}

func (f *fakeConn) Ping(ctx context.Context) error {
	f.mu.Lock()
	f.pings++
	f.mu.Unlock()
	if f.ping != nil {
		return f.ping(ctx)
	}
	return nil
}

func (f *fakeConn) SetReadLimit(n int64) {
	f.mu.Lock()
	f.readLimit = n
	f.mu.Unlock()
}

func (f *fakeConn) closeInfo() (int, string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeCode, f.closeReason, f.closeCount
}

func (f *fakeConn) pingCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pings
}

func newTestServer() *Server {
	s := NewServer()
	s.SetLogger(NewNoopLogger())
	return s
}

func connectFake(t *testing.T, ns *Namespace, id string, f *fakeConn) *ConnContext {
	t.Helper()
	ctx := ns.addConn(id, f, httptest.NewRequest("GET", "/", nil))
	if ctx == nil {
		t.Fatalf("addConn(%s) rejected", id)
	}
	return ctx
}

func waitUntil(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func countDisconnects(ns *Namespace) *atomic.Int32 {
	var n atomic.Int32
	ns.OnDisconnect(func(*ConnContext) { n.Add(1) })
	return &n
}

func TestHeartbeatClosesWithoutPong(t *testing.T) {
	s := newTestServer()
	s.SetPingInterval(20 * time.Millisecond)
	s.SetPongTimeout(30 * time.Millisecond)
	ns := s.Of("/")
	disc := countDisconnects(ns)

	f := newFakeConn()
	f.ping = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	start := time.Now()
	connectFake(t, ns, "c1", f)

	if !waitUntil(time.Second, func() bool { return disc.Load() == 1 }) {
		t.Fatal("connection without pong was not closed")
	}
	if el := time.Since(start); el < 45*time.Millisecond {
		t.Errorf("closed after %v, want >= interval+pongTimeout", el)
	}
	time.Sleep(60 * time.Millisecond)
	if got := disc.Load(); got != 1 {
		t.Errorf("OnDisconnect called %d times, want 1", got)
	}
	if ns.ConnectionCount() != 0 {
		t.Error("connection should be removed")
	}
}

func TestHeartbeatHealthyConnStays(t *testing.T) {
	s := newTestServer()
	s.SetPingInterval(20 * time.Millisecond)
	s.SetPongTimeout(30 * time.Millisecond)
	ns := s.Of("/")
	disc := countDisconnects(ns)

	f := newFakeConn()
	ctx := connectFake(t, ns, "c1", f)
	defer ctx.Close()

	if !waitUntil(time.Second, func() bool { return f.pingCount() >= 3 }) {
		t.Fatalf("pings = %d, want >= 3", f.pingCount())
	}
	if disc.Load() != 0 || ns.ConnectionCount() != 1 {
		t.Error("healthy connection must stay open")
	}
}

func TestHeartbeatDisabled(t *testing.T) {
	s := newTestServer()
	s.SetPingInterval(0)
	ns := s.Of("/")

	f := newFakeConn()
	ctx := connectFake(t, ns, "c1", f)
	defer ctx.Close()

	time.Sleep(50 * time.Millisecond)
	if f.pingCount() != 0 {
		t.Errorf("pings = %d, want 0 when heartbeat disabled", f.pingCount())
	}
}

func TestWriteErrorClosesConn(t *testing.T) {
	s := newTestServer()
	ns := s.Of("/")
	disc := countDisconnects(ns)

	f := newFakeConn()
	f.write = func(any) error { return context.DeadlineExceeded }
	ctx := connectFake(t, ns, "c1", f)

	_ = ctx.Emit("x", nil)
	if !waitUntil(time.Second, func() bool { return disc.Load() == 1 }) {
		t.Fatal("stuck write should close the connection")
	}
	if _, _, n := f.closeInfo(); n == 0 {
		t.Error("WSConn.Close should be called")
	}
}

func TestConnGoroutinesStopAfterClose(t *testing.T) {
	s := newTestServer()
	s.SetPingInterval(10 * time.Millisecond)
	s.SetPongTimeout(10 * time.Millisecond)
	ns := s.Of("/")
	disc := countDisconnects(ns)

	base := runtime.NumGoroutine()
	ctxs := make([]*ConnContext, 0, 20)
	for i := 0; i < 20; i++ {
		ctxs = append(ctxs, connectFake(t, ns, generateId(), newFakeConn()))
	}
	time.Sleep(30 * time.Millisecond)
	for _, c := range ctxs {
		c.Close()
	}
	if !waitUntil(time.Second, func() bool { return disc.Load() == 20 }) {
		t.Fatalf("disconnects = %d, want 20", disc.Load())
	}
	if !waitUntil(time.Second, func() bool { return runtime.NumGoroutine() <= base }) {
		t.Errorf("goroutines = %d, want <= %d", runtime.NumGoroutine(), base)
	}
}

func TestServerTimeoutDefaults(t *testing.T) {
	for _, s := range []*Server{NewServer(), NewFiberServer()} {
		if s.pingInterval != DefaultPingInterval || s.pongTimeout != DefaultPongTimeout ||
			s.writeTimeout != DefaultWriteTimeout || s.readLimit != DefaultReadLimit {
			t.Errorf("defaults not applied: %v %v %v %v", s.pingInterval, s.pongTimeout, s.writeTimeout, s.readLimit)
		}
	}
}

// recordLogger keeps Error and Warn messages for assertions
type recordLogger struct {
	mu     sync.Mutex
	errors []string
	warns  []string
}

func (l *recordLogger) Debug(string, ...interface{}) {}
func (l *recordLogger) Info(string, ...interface{})  {}
func (l *recordLogger) Warn(msg string, _ ...interface{}) {
	l.mu.Lock()
	l.warns = append(l.warns, msg)
	l.mu.Unlock()
}
func (l *recordLogger) Error(msg string, _ ...interface{}) {
	l.mu.Lock()
	l.errors = append(l.errors, msg)
	l.mu.Unlock()
}

func (l *recordLogger) counts() (errs, warns int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.errors), len(l.warns)
}

func TestEmitConcurrentWithClose(t *testing.T) {
	s := newTestServer()
	s.SetPingInterval(0)
	ns := s.Of("/")
	disc := countDisconnects(ns)

	const conns = 20
	for i := 0; i < conns; i++ {
		ctx := connectFake(t, ns, generateId(), newFakeConn())
		var wg sync.WaitGroup
		for g := 0; g < 20; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					_ = ctx.Emit("e", j)
				}
			}()
		}
		go ctx.Close()
		wg.Wait()
	}
	if !waitUntil(time.Second, func() bool { return disc.Load() == conns }) {
		t.Errorf("disconnects = %d, want %d", disc.Load(), conns)
	}
}

func TestAdapterHandlerPanicRecovered(t *testing.T) {
	s := newTestServer()
	logger := &recordLogger{}
	s.SetLogger(logger)
	adapter := &mockAdapter{}
	s.SetAdapter(adapter)
	s.SetPingInterval(0)
	ns := s.Of("/")
	disc := countDisconnects(ns)

	ctx := connectFake(t, ns, "c1", newFakeConn())
	ctx.Local("k", map[string]any{})

	// comparing two maps inside the key-value filter panics
	adapter.deliver(BroadcastData{FromServerID: "other", Key: "k", Value: map[string]any{"a": 1.0}})

	if errs, _ := logger.counts(); errs == 0 {
		t.Error("panic in adapter handler should be logged as error")
	}

	// namespace must stay usable (no lock leaked by the panic)
	done := make(chan struct{})
	go func() { ctx.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("namespace deadlocked after adapter handler panic")
	}
	if !waitUntil(time.Second, func() bool { return disc.Load() == 1 }) {
		t.Error("connection should close normally after recovered panic")
	}
}

func TestAddConnSetsReadLimit(t *testing.T) {
	s := newTestServer()
	s.SetPingInterval(0)
	s.SetReadLimit(4096)
	f := newFakeConn()
	ctx := connectFake(t, s.Of("/"), "c1", f)
	defer ctx.Close()

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readLimit != 4096 {
		t.Errorf("WSConn read limit = %d, want 4096", f.readLimit)
	}
}

// received collects messages written to f within d
func received(f *fakeConn, d time.Duration) []Message {
	var msgs []Message
	timeout := time.After(d)
	for {
		select {
		case m := <-f.out:
			msgs = append(msgs, m)
		case <-timeout:
			return msgs
		}
	}
}

// roomFixture: c1 in r1+r2, c2 in r2, c3 in r3
func roomFixture(t *testing.T) (*Namespace, *mockAdapter, map[string]*fakeConn) {
	t.Helper()
	s := newTestServer()
	s.SetPingInterval(0)
	adapter := &mockAdapter{}
	s.SetAdapter(adapter)
	ns := s.Of("/")
	conns := map[string]*fakeConn{}
	for id, rooms := range map[string][]string{"c1": {"r1", "r2"}, "c2": {"r2"}, "c3": {"r3"}} {
		f := newFakeConn()
		ctx := connectFake(t, ns, id, f)
		for _, r := range rooms {
			ctx.JoinRoom(r)
		}
		conns[id] = f
		t.Cleanup(ctx.Close)
	}
	return ns, adapter, conns
}

func assertReceived(t *testing.T, conns map[string]*fakeConn, want map[string]int) {
	t.Helper()
	for id, f := range conns {
		if got := len(received(f, 50*time.Millisecond)); got != want[id] {
			t.Errorf("%s received %d messages, want %d", id, got, want[id])
		}
	}
}

func TestEmitToRoomsSingleBroadcast(t *testing.T) {
	ns, adapter, _ := roomFixture(t)
	rooms := make([]string, 500)
	for i := range rooms {
		rooms[i] = generateId()
	}

	ns.EmitToRooms(rooms, Message{Event: "e"})

	sent := adapter.sent()
	if len(sent) != 1 {
		t.Fatalf("adapter Broadcast called %d times, want 1", len(sent))
	}
	if len(sent[0].Rooms) != 500 {
		t.Errorf("payload Rooms = %d, want 500", len(sent[0].Rooms))
	}
	// an old node ignores Rooms: it must take the key filter path that matches nobody,
	// never the broadcast-to-all path
	if sent[0].Room != "" || sent[0].Key == "" || sent[0].Value == nil {
		t.Errorf("payload would be broadcast to all by old nodes: %+v", sent[0])
	}
}

func TestEmitToRoomsDedupeLocal(t *testing.T) {
	ns, _, conns := roomFixture(t)
	ns.EmitToRooms([]string{"r1", "r2"}, Message{Event: "e"})
	assertReceived(t, conns, map[string]int{"c1": 1, "c2": 1, "c3": 0})
}

func TestEmitToRoomsEmptyIsNoop(t *testing.T) {
	ns, adapter, conns := roomFixture(t)
	ns.EmitToRooms(nil, Message{Event: "e"})
	if len(adapter.sent()) != 0 {
		t.Error("empty rooms must not publish (receivers would treat it as broadcast)")
	}
	assertReceived(t, conns, map[string]int{})
}

func TestAdapterRoomsPayload(t *testing.T) {
	ns, adapter, conns := roomFixture(t)
	_ = ns
	adapter.deliver(BroadcastData{
		FromServerID: "other",
		Rooms:        []string{"r1", "r2"},
		Key:          legacyGuardKey,
		Value:        true,
		Message:      MessageMultiNodes{Message: Message{Event: "e"}},
	})
	assertReceived(t, conns, map[string]int{"c1": 1, "c2": 1, "c3": 0})
}

func TestAdapterSingleRoomPayload(t *testing.T) {
	_, adapter, conns := roomFixture(t)
	adapter.deliver(BroadcastData{
		FromServerID: "other",
		Room:         "r2",
		Message:      MessageMultiNodes{Message: Message{Event: "e"}},
	})
	assertReceived(t, conns, map[string]int{"c1": 1, "c2": 1, "c3": 0})
}

func assertClosed(t *testing.T, f *fakeConn, id string, code int, reason string) {
	t.Helper()
	if !waitUntil(time.Second, func() bool { _, _, n := f.closeInfo(); return n > 0 }) {
		t.Fatalf("%s should be closed", id)
	}
	if c, r, _ := f.closeInfo(); c != code || r != reason {
		t.Errorf("%s closed with %d %q, want %d %q", id, c, r, code, reason)
	}
}

func assertOpen(t *testing.T, f *fakeConn, id string) {
	t.Helper()
	time.Sleep(30 * time.Millisecond)
	if _, _, n := f.closeInfo(); n != 0 {
		t.Errorf("%s outside target rooms must stay open", id)
	}
}

func TestCloseRoomsLocal(t *testing.T) {
	ns, adapter, conns := roomFixture(t)
	ns.CloseRooms([]string{"r1"}, 4401, "unauthorized")

	assertClosed(t, conns["c1"], "c1", 4401, "unauthorized")
	assertOpen(t, conns["c2"], "c2")
	assertOpen(t, conns["c3"], "c3")

	sent := adapter.sent()
	if len(sent) != 1 {
		t.Fatalf("adapter Broadcast called %d times, want 1", len(sent))
	}
	d := sent[0]
	if d.Action != BroadcastActionClose || len(d.Rooms) != 1 || d.Code != 4401 || d.Reason != "unauthorized" {
		t.Errorf("unexpected close payload %+v", d)
	}
	if d.Key == "" || d.Value == nil {
		t.Errorf("close payload would be broadcast to all by old nodes: %+v", d)
	}
}

func TestCloseRoomsFromAdapter(t *testing.T) {
	_, adapter, conns := roomFixture(t)
	adapter.deliver(BroadcastData{
		FromServerID: "other",
		Action:       BroadcastActionClose,
		Rooms:        []string{"r2"},
		Code:         4403,
		Reason:       "kicked",
		Key:          legacyGuardKey,
		Value:        true,
	})
	assertClosed(t, conns["c1"], "c1", 4403, "kicked")
	assertClosed(t, conns["c2"], "c2", 4403, "kicked")
	assertOpen(t, conns["c3"], "c3")
}

func TestCloseWithTwice(t *testing.T) {
	s := newTestServer()
	s.SetPingInterval(0)
	ns := s.Of("/")
	disc := countDisconnects(ns)
	f := newFakeConn()
	ctx := connectFake(t, ns, "c1", f)

	ctx.CloseWith(4000, "bye")
	ctx.CloseWith(4001, "again")

	assertClosed(t, f, "c1", 4000, "bye")
	time.Sleep(30 * time.Millisecond)
	if got := disc.Load(); got != 1 {
		t.Errorf("OnDisconnect called %d times, want 1", got)
	}
	if _, _, n := f.closeInfo(); n != 1 {
		t.Errorf("WSConn.Close called %d times, want 1", n)
	}
}

func TestCloseWithInvalidCode(t *testing.T) {
	s := newTestServer()
	logger := &recordLogger{}
	s.SetLogger(logger)
	s.SetPingInterval(0)
	ns := s.Of("/")
	f := newFakeConn()
	ctx := connectFake(t, ns, "c1", f)

	ctx.CloseWith(1006, "abnormal")

	assertClosed(t, f, "c1", 1000, "abnormal")
	if _, warns := logger.counts(); warns == 0 {
		t.Error("invalid close code should log a warning")
	}
}

func TestCloseDefault(t *testing.T) {
	s := newTestServer()
	s.SetPingInterval(0)
	f := newFakeConn()
	ctx := connectFake(t, s.Of("/"), "c1", f)
	ctx.Close()
	assertClosed(t, f, "c1", 1000, "connection closed")
}
