package tap

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
)

var workers sync.Map

var writerGroups = struct {
	sync.Mutex
	byPath map[string]*tapRuntime
}{byPath: make(map[string]*tapRuntime)}

const activeTapsVar = "http.handlers.tap.active"

func init() {
	caddy.OnExit(drainWorkers)
	caddy.RegisterModule(Tap{})
	httpcaddyfile.RegisterHandlerDirective("tap", func(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
		tap := new(Tap)
		return tap, tap.UnmarshalCaddyfile(h.Dispenser)
	})
	httpcaddyfile.RegisterDirectiveOrder("tap", httpcaddyfile.Before, "header")
}

func drainWorkers(_ context.Context) {
	workers.Range(func(key, _ any) bool {
		rt := key.(*tapRuntime)
		rt.mu.Lock()
		rt.stopping = true
		rt.closeQueue()
		rt.mu.Unlock()
		<-rt.done
		return true
	})
}

// Tap records HTTP exchanges at its position in the handler chain.
type Tap struct {
	Identity     string         `json:"identity"`
	Directory    string         `json:"directory"`
	Mode         string         `json:"mode,omitempty"`
	QueueSize    int            `json:"queue_size,omitempty"`
	MaxBodyBytes int64          `json:"max_body_bytes,omitempty"`
	RollSize     int64          `json:"roll_size,omitempty"`
	RollInterval caddy.Duration `json:"roll_interval,omitempty"`
	RollKeep     int            `json:"roll_keep,omitempty"`
	RollKeepFor  caddy.Duration `json:"roll_keep_for,omitempty"`

	runtime  *tapRuntime
	released bool // guarded by runtime.mu
}

type writerSettings struct {
	queueSize int
	sizeLimit int64
	interval  time.Duration
	keep      int
	maxAge    time.Duration
}

type tapRuntime struct {
	settings   writerSettings
	refs       int
	mu         sync.Mutex
	stopping   bool
	active     int
	closed     bool
	slots      chan struct{}
	queue      chan queuedRecord
	done       chan struct{}
	dropped    atomic.Uint64
	dropNotice chan struct{}
	logger     *zap.Logger
}

type queuedRecord struct {
	record exchange
	result chan error
}

func (Tap) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.tap",
		New: func() caddy.Module { return new(Tap) },
	}
}

var validIdentity = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

func (t *Tap) Validate() error {
	switch {
	case !validIdentity.MatchString(t.Identity):
		return fmt.Errorf("identity must be 1–64 ASCII letters, digits, underscores or hyphens and start with a letter or digit")
	case t.Directory == "":
		return fmt.Errorf("directory is required")
	case t.Mode != "" && t.Mode != "drop" && t.Mode != "strict":
		return fmt.Errorf("mode must be drop or strict")
	case t.QueueSize < 0 || t.QueueSize > 65536:
		return fmt.Errorf("queue_size must be between 1 and 65536 when set")
	case t.MaxBodyBytes < 0, t.RollSize < 0, t.RollInterval < 0:
		return fmt.Errorf("max_body_bytes, roll_size and roll_interval must be nonnegative")
	case t.Mode == "strict" && t.MaxBodyBytes != 0:
		return fmt.Errorf("max_body_bytes cannot be set in strict mode")
	case t.RollKeep < -1 || t.RollKeepFor < -1:
		return fmt.Errorf("retention limits must be nonnegative or -1 to disable")
	}
	return nil
}

func (t *Tap) Provision(ctx caddy.Context) error {
	if err := t.Validate(); err != nil {
		return err
	}
	if t.QueueSize == 0 {
		t.QueueSize = 64
	}
	if t.Mode != "strict" && t.MaxBodyBytes == 0 {
		t.MaxBodyBytes = 1 << 20
	}
	if t.RollSize == 0 {
		t.RollSize = 100 << 20
	}
	if t.RollKeep == 0 {
		t.RollKeep = 10
	}
	if t.RollKeepFor == 0 {
		t.RollKeepFor = caddy.Duration(90 * 24 * time.Hour)
	}
	dir, err := filepath.Abs(t.Directory)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return err
	}
	settings := writerSettings{t.QueueSize, t.RollSize, time.Duration(t.RollInterval), t.RollKeep, time.Duration(t.RollKeepFor)}
	lockPath := filepath.Join(dir, ".tap-"+t.Identity+".lock")
	for {
		writerGroups.Lock()
		rt := writerGroups.byPath[lockPath]
		if rt == nil {
			defer writerGroups.Unlock()
			break
		}
		rt.mu.Lock()
		if rt.closed {
			rt.mu.Unlock()
			writerGroups.Unlock()
			<-rt.done
			continue
		}
		if rt.settings != settings {
			rt.mu.Unlock()
			writerGroups.Unlock()
			return fmt.Errorf("tap %q in %s has conflicting queue or rotation/retention settings", t.Identity, dir)
		}
		rt.refs++
		rt.stopping = false
		t.runtime = rt
		rt.mu.Unlock()
		writerGroups.Unlock()
		return nil
	}
	logger := ctx.Logger().With(zap.String("identity", t.Identity))
	rt := &tapRuntime{
		settings: settings, refs: 1,
		slots:      make(chan struct{}, t.QueueSize),
		queue:      make(chan queuedRecord, t.QueueSize),
		done:       make(chan struct{}),
		dropNotice: make(chan struct{}, 1),
		logger:     logger,
	}
	t.runtime = rt
	sink := fileSink{
		dir: dir, prefix: t.Identity + "-",
		sizeLimit: settings.sizeLimit, interval: settings.interval,
		keep: settings.keep, maxAge: settings.maxAge,
		logger: logger,
	}
	writerGroups.byPath[lockPath] = rt
	workers.Store(rt, struct{}{})
	go rt.writeRecords(&sink, lockPath)
	return nil
}

func lockWriter(path string) (*os.File, error) {
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := tryLock(lock); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("could not lock tap log group %q: %w", path, err)
	}
	return lock, nil
}

func (rt *tapRuntime) writeRecords(sink *fileSink, lockPath string) {
	defer close(rt.done)
	defer workers.Delete(rt)
	var lock *os.File
	if lockPath != "" {
		defer func() {
			writerGroups.Lock()
			if lock != nil {
				if err := lock.Close(); err != nil {
					rt.logger.Error("could not release tap log group lock", zap.Error(err))
				}
			}
			delete(writerGroups.byPath, lockPath)
			writerGroups.Unlock()
		}()
	}
	defer func() { sink.reportClose(time.Now()) }()
	timer := time.NewTimer(0)
	timer.Stop()
	defer timer.Stop()
	var reportedDrops uint64
	reportDrops := func() {
		if dropped := rt.dropped.Load(); dropped > reportedDrops {
			rt.logger.Warn("tap capacity exhausted, HTTP exchanges dropped", zap.Uint64("dropped_total", dropped), zap.Uint64("dropped_since_last_warning", dropped-reportedDrops))
			reportedDrops = dropped
		}
	}
	defer reportDrops()
	for {
		var expiry <-chan time.Time
		if sink.file != nil && sink.interval > 0 {
			timer.Reset(time.Until(sink.opened.Add(sink.interval)))
			expiry = timer.C
		}
		select {
		case now := <-expiry:
			sink.reportClose(now)
		case <-rt.dropNotice:
			reportDrops()
		case item, ok := <-rt.queue:
			if !ok {
				return
			}
			var err error
			if lock == nil && lockPath != "" {
				// Caddy also provisions handlers during config validation.
				lock, err = lockWriter(lockPath)
				if err == nil {
					sink.recover(time.Now())
				}
			}
			if err == nil {
				sink.strict = item.result != nil
				err = sink.write(time.Now(), item.record)
			}
			if err != nil {
				rt.logger.Error("could not write HTTP exchange", zap.Error(err))
			}
			<-rt.slots
			if item.result != nil {
				item.result <- err
			}
		}
	}
}

func (t *Tap) Cleanup() error {
	if t.runtime == nil {
		return nil
	}
	rt := t.runtime
	rt.mu.Lock()
	if !t.released {
		t.released = true
		if rt.refs > 0 {
			rt.refs--
		}
	}
	if rt.refs != 0 {
		rt.mu.Unlock()
		return nil
	}
	rt.stopping = true
	idle := rt.active == 0
	if idle {
		rt.closeQueue()
	}
	rt.mu.Unlock()
	// Caddy reloads can run cleanup before a downstream module closes its streams.
	if idle {
		<-rt.done
	}
	return nil
}

// closeQueue runs with rt.mu held.
func (rt *tapRuntime) closeQueue() {
	if !rt.closed {
		rt.closed = true
		close(rt.queue)
	}
}

func (t *Tap) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	rt := t.runtime
	vars, _ := r.Context().Value(caddyhttp.VarsCtxKey).(map[string]any)
	activeTaps, _ := vars[activeTapsVar].(map[*tapRuntime]bool)
	if activeTaps[rt] {
		return next.ServeHTTP(w, r)
	}
	strict := t.Mode == "strict"
	rt.mu.Lock()
	if t.released || rt.stopping {
		rt.mu.Unlock()
		return next.ServeHTTP(w, r)
	}
	if !strict {
		select {
		case rt.slots <- struct{}{}:
		default:
			if dropped := rt.dropped.Add(1); dropped&(dropped-1) == 0 {
				select {
				case rt.dropNotice <- struct{}{}:
				default:
				}
			}
			rt.mu.Unlock()
			return next.ServeHTTP(w, r)
		}
	}
	rt.active++
	rt.mu.Unlock()

	defer func() {
		rt.mu.Lock()
		rt.active--
		if rt.stopping && rt.active == 0 {
			rt.closeQueue()
		}
		rt.mu.Unlock()
	}()

	item := queuedRecord{}
	if strict {
		ctx := r.Context()
		if len(activeTaps) != 0 {
			// An outer capture holds a slot while this handler runs.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case rt.slots <- struct{}{}:
			default:
				return caddyhttp.Error(http.StatusServiceUnavailable, fmt.Errorf("tap %q has no capture slot available for nested strict capture", t.Identity))
			}
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case rt.slots <- struct{}{}:
			}
		}
		if err := ctx.Err(); err != nil {
			<-rt.slots
			return err
		}
		item.result = make(chan error, 1)
	}
	// A nested route using this writer would otherwise wait on its own strict slot.
	if vars != nil {
		if activeTaps == nil {
			activeTaps = make(map[*tapRuntime]bool)
			vars[activeTapsVar] = activeTaps
		}
		activeTaps[rt] = true
		defer delete(activeTaps, rt)
	}
	var persistErr error
	handlerErr := capture(w, r, next, t.MaxBodyBytes, t.Identity, func(record exchange) {
		item.record = record
		rt.mu.Lock()
		if rt.closed {
			rt.mu.Unlock()
			<-rt.slots
			persistErr = fmt.Errorf("tap queue closed during process exit")
			return
		}
		rt.queue <- item
		rt.mu.Unlock()
		if strict {
			persistErr = <-item.result
		}
	})
	if strict && persistErr != nil && handlerErr == nil && item.record.Response.Status == 0 && !item.record.Response.Hijacked {
		return caddyhttp.Error(http.StatusInternalServerError, fmt.Errorf("tap persistence: %w", persistErr))
	}
	return handlerErr
}

var (
	_ caddy.Module                = Tap{}
	_ caddy.Provisioner           = (*Tap)(nil)
	_ caddy.Validator             = (*Tap)(nil)
	_ caddy.CleanerUpper          = (*Tap)(nil)
	_ caddyhttp.MiddlewareHandler = (*Tap)(nil)
	_ caddyfile.Unmarshaler       = (*Tap)(nil)
)
