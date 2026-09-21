package embed

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/metrics"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// The decision log is the whole durable record of which cross-namespace groups committed.
//
// It is deliberately tiny and dumb: a 16-byte header, then one fixed-size record per committed
// group - the 16-byte group id and a CRC32C over it. No framing, no compression, no index. A
// group commits the instant its record is durable, and every participant commit it names is
// already durable by then (see TxnGroup.Finish), so this file never has to say anything about
// the participants themselves: it only has to answer "did group G commit".
//
// One file per epoch; see TxnCoordinator for what an epoch is and how files are sealed.

const (
	txnDirName            = "txn"
	decisionHeaderMagic   = "KDBXNSD1"
	decisionHeaderSize    = 16
	decisionRecordSize    = 20
	decisionMaxBatch      = 512
	decisionLiveSuffix    = ".log"
	decisionDeadSuffix    = ".dead"
	decisionFilePrefix    = "decisions-"
	epochFileName         = "EPOCH"
	hostFileName          = "HOST"
	decisionRollThreshold = 1 << 20
)

var crc32c = crc32.MakeTable(crc32.Castagnoli)

// ErrDecisionLogClosed is returned to a group that tries to commit after its host has closed.
var ErrDecisionLogClosed = errors.New("kdb: cross-namespace decision log is closed")

func txnDir(dataRoot string) string { return filepath.Join(dataRoot, txnDirName) }

func decisionPath(dataRoot string, epoch uint64, suffix string) string {
	return filepath.Join(txnDir(dataRoot), fmt.Sprintf("%s%016d%s", decisionFilePrefix, epoch, suffix))
}

// parseDecisionFileName returns the epoch and suffix of a decision file name, or ok=false for
// anything else in the directory.
func parseDecisionFileName(name string) (epoch uint64, suffix string, ok bool) {
	if !strings.HasPrefix(name, decisionFilePrefix) {
		return 0, "", false
	}
	rest := strings.TrimPrefix(name, decisionFilePrefix)
	for _, s := range []string{decisionLiveSuffix, decisionDeadSuffix} {
		if strings.HasSuffix(rest, s) {
			n, err := strconv.ParseUint(strings.TrimSuffix(rest, s), 10, 64)
			if err != nil {
				return 0, "", false
			}
			return n, s, true
		}
	}
	return 0, "", false
}

func encodeDecisionRecord(dst []byte, id codec.UUID) {
	binary.BigEndian.PutUint64(dst[0:8], uint64(id.MSB))
	binary.BigEndian.PutUint64(dst[8:16], uint64(id.LSB))
	binary.BigEndian.PutUint32(dst[16:20], crc32.Checksum(dst[0:16], crc32c))
}

// readDecisions reads every intact record of a decision file. A short or corrupt record ends the
// read without an error: it can only be the torn tail of a write that was never acknowledged,
// because a group is not committed until its record's fsync returns, and records are appended
// strictly in order.
func readDecisions(path string) (map[codec.UUID]struct{}, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := make(map[codec.UUID]struct{})
	if len(raw) < decisionHeaderSize {
		// Created but the header never made it: an epoch that committed nothing.
		return out, nil
	}
	if string(raw[:8]) != decisionHeaderMagic {
		return nil, fmt.Errorf("kdb: %s is not a cross-namespace decision log", path)
	}
	for off := decisionHeaderSize; off+decisionRecordSize <= len(raw); off += decisionRecordSize {
		rec := raw[off : off+decisionRecordSize]
		if crc32.Checksum(rec[0:16], crc32c) != binary.BigEndian.Uint32(rec[16:20]) {
			break
		}
		id := codec.UUID{
			MSB: int64(binary.BigEndian.Uint64(rec[0:8])),
			LSB: int64(binary.BigEndian.Uint64(rec[8:16])),
		}
		out[id] = struct{}{}
	}
	return out, nil
}

// syncDir makes a create, rename or remove inside dir durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// readEpochFile returns the last epoch handed out, or 0 when none ever was.
func readEpochFile(dataRoot string) (uint64, error) {
	raw, err := os.ReadFile(filepath.Join(txnDir(dataRoot), epochFileName))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("kdb: unreadable %s: %w", epochFileName, err)
	}
	return n, nil
}

func writeEpochFile(dataRoot string, epoch uint64) error {
	dir := txnDir(dataRoot)
	tmp := filepath.Join(dir, epochFileName+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(f, strconv.FormatUint(epoch, 10)+"\n"); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, epochFileName)); err != nil {
		return err
	}
	return syncDir(dir)
}

// decisionLog appends decision records for one epoch from a single goroutine, sharing one fsync
// across every record queued while the previous one ran - the same group commit the namespace
// commit log does (see commitLogWriter), for the same reason: without it, every cross-namespace
// commit would pay its own full fsync in strict sequence.
type decisionLog struct {
	path string
	f    *os.File
	// syncMode is the host's: a decision is exactly as durable as the parts it commits, no more
	// (a full device flush behind parts that only had a barrier buys nothing) and no less.
	syncMode storio.SyncMode

	reqs chan *decisionReq
	done chan struct{}

	// sendMu makes closing reqs safe against a concurrent enqueue: enqueue holds it for read
	// across its send, close takes it for write. The drain goroutine never takes it, so a sender
	// blocked on a full channel cannot deadlock a close.
	sendMu sync.RWMutex
	closed bool

	failure atomic.Pointer[error]
	size    atomic.Int64
}

type decisionReq struct {
	id  codec.UUID
	ack chan error
}

func createDecisionLog(path string, epoch uint64, syncMode storio.SyncMode) (*decisionLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	header := make([]byte, decisionHeaderSize)
	copy(header, decisionHeaderMagic)
	binary.BigEndian.PutUint64(header[8:16], epoch)
	if _, err := f.Write(header); err != nil {
		f.Close()
		return nil, err
	}
	if err := storio.SyncFile(f, syncMode); err != nil {
		f.Close()
		return nil, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		f.Close()
		return nil, err
	}
	l := &decisionLog{
		path:     path,
		f:        f,
		syncMode: syncMode,
		reqs:     make(chan *decisionReq, decisionMaxBatch),
		done:     make(chan struct{}),
	}
	l.size.Store(decisionHeaderSize)
	go l.run()
	return l, nil
}

func (l *decisionLog) latched() error {
	if p := l.failure.Load(); p != nil {
		return *p
	}
	return nil
}

// enqueue queues id's record and returns a wait that reports when it is durable. Records become
// durable in enqueue order - a later batch is never fsynced before an earlier one - which is what
// lets a group behind another group wait only for the earlier decision to be queued.
func (l *decisionLog) enqueue(id codec.UUID) (wait func() error, err error) {
	if err := l.latched(); err != nil {
		return nil, err
	}
	req := &decisionReq{id: id, ack: make(chan error, 1)}
	l.sendMu.RLock()
	if l.closed {
		l.sendMu.RUnlock()
		return nil, ErrDecisionLogClosed
	}
	l.reqs <- req
	l.sendMu.RUnlock()
	return func() error { return <-req.ack }, nil
}

func (l *decisionLog) run() {
	defer close(l.done)
	buf := make([]byte, 0, decisionMaxBatch*decisionRecordSize)
	for first := range l.reqs {
		batch := []*decisionReq{first}
	drain:
		for len(batch) < decisionMaxBatch {
			select {
			case r, ok := <-l.reqs:
				if !ok {
					break drain
				}
				batch = append(batch, r)
			default:
				break drain
			}
		}
		err := l.latched()
		if err == nil {
			buf = buf[:len(batch)*decisionRecordSize]
			for i, r := range batch {
				encodeDecisionRecord(buf[i*decisionRecordSize:], r.id)
			}
			if _, werr := l.f.Write(buf); werr != nil {
				err = fmt.Errorf("appending to %s: %w", l.path, werr)
			} else {
				stop := metrics.Default.Track(metrics.StageFsyncWait)
				if serr := storio.SyncFile(l.f, l.syncMode); serr != nil {
					err = fmt.Errorf("syncing %s: %w", l.path, serr)
				}
				stop()
				l.size.Add(int64(len(buf)))
			}
			if err != nil {
				l.failure.CompareAndSwap(nil, &err)
			}
		}
		for _, r := range batch {
			r.ack <- err
		}
	}
}

// close stops accepting records, waits for every queued one to be written, and closes the file.
func (l *decisionLog) close() error {
	l.sendMu.Lock()
	if !l.closed {
		l.closed = true
		close(l.reqs)
	}
	l.sendMu.Unlock()
	<-l.done
	cerr := l.f.Close()
	if err := l.latched(); err != nil {
		return err
	}
	return cerr
}

// readHostID returns the data root's cross-namespace host id, or "" when none was ever assigned -
// which is the case for every data root that has never run a cross-namespace transaction.
func readHostID(dataRoot string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(txnDir(dataRoot), hostFileName))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(raw)), nil
}

// writeHostID assigns the data root a host id, durably, before any marker names it.
func writeHostID(dataRoot, id string) error {
	dir := txnDir(dataRoot)
	tmp := filepath.Join(dir, hostFileName+".tmp")
	if err := os.WriteFile(tmp, []byte(id+"\n"), 0o644); err != nil {
		return err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	serr := f.Sync()
	f.Close()
	if serr != nil {
		return serr
	}
	if err := os.Rename(tmp, filepath.Join(dir, hostFileName)); err != nil {
		return err
	}
	return syncDir(dir)
}
