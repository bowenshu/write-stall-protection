// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage/internal/apiv2/storagepb"
	gax "github.com/googleapis/gax-go/v2"
	"github.com/googleapis/gax-go/v2/callctx"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestGetObjectChecksums(t *testing.T) {
	tests := []struct {
		name                string
		fullObjectChecksum  func() *uint32
		finishWrite         bool
		sendCRC32C          bool
		disableAutoChecksum bool
		attrs               *ObjectAttrs
		append              bool
		want                *storagepb.ObjectChecksums
	}{
		{
			name:        "finishWrite is false",
			finishWrite: false,
			want:        nil,
		},
		{
			name:        "objectAttrs is nil",
			finishWrite: true,
			want:        nil,
		},
		{
			name:        "sendCRC32C is true, attrs have CRC32C",
			finishWrite: true,
			sendCRC32C:  true,
			attrs:       &ObjectAttrs{CRC32C: 123},
			want: &storagepb.ObjectChecksums{
				Crc32C: proto.Uint32(123),
			},
		},
		{
			name:                "disableCRC32C is true and sendCRC32C is true",
			finishWrite:         true,
			sendCRC32C:          true,
			disableAutoChecksum: true,
			attrs:               &ObjectAttrs{CRC32C: 123},
			want: &storagepb.ObjectChecksums{
				Crc32C: proto.Uint32(123),
			},
		},
		{
			name:        "sendCRC32C is true",
			finishWrite: true,
			sendCRC32C:  true,
			attrs:       &ObjectAttrs{CRC32C: 123},
			want: &storagepb.ObjectChecksums{
				Crc32C: proto.Uint32(123),
			},
		},
		{
			name:        "MD5 is provided",
			finishWrite: true,
			attrs:       &ObjectAttrs{MD5: []byte{1, 5, 0}},
			want: &storagepb.ObjectChecksums{
				Md5Hash: []byte{1, 5, 0},
			},
		},
		{
			name:                "disableCRC32C is true and sendCRC32C is false",
			finishWrite:         true,
			sendCRC32C:          false,
			disableAutoChecksum: true,
			want:                nil,
		},
		{
			name:                "CRC32C enabled, no user-provided checksum",
			fullObjectChecksum:  func() *uint32 { return proto.Uint32(456) },
			finishWrite:         true,
			sendCRC32C:          false,
			disableAutoChecksum: false,
			attrs:               &ObjectAttrs{},
			want: &storagepb.ObjectChecksums{
				Crc32C: proto.Uint32(456),
			},
		},
		{
			name:                "CRC32C enabled, but callback returns nil (missing initial checksum)",
			fullObjectChecksum:  func() *uint32 { return nil },
			finishWrite:         true,
			sendCRC32C:          false,
			disableAutoChecksum: false,
			attrs:               &ObjectAttrs{},
			want:                nil,
		},
		{
			name:                "Append operation without final user-provided CRC32C (callback returns nil)",
			fullObjectChecksum:  func() *uint32 { return nil },
			finishWrite:         true,
			append:              true,
			sendCRC32C:          false,
			disableAutoChecksum: false,
			attrs:               &ObjectAttrs{},
			want:                nil,
		},
		{
			name:                "Append operation with final CRC32C and initial CRC32C",
			fullObjectChecksum:  func() *uint32 { return proto.Uint32(123) },
			finishWrite:         true,
			append:              true,
			sendCRC32C:          false,
			disableAutoChecksum: false,
			attrs:               &ObjectAttrs{CRC32C: 456},
			want: &storagepb.ObjectChecksums{
				Crc32C: proto.Uint32(123),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getObjectChecksums(&getObjectChecksumsParams{
				disableAutoChecksum: tt.disableAutoChecksum,
				sendCRC32C:          tt.sendCRC32C,
				objectAttrs:         tt.attrs,
				fullObjectChecksum:  tt.fullObjectChecksum,
				finishWrite:         tt.finishWrite,
				append:              tt.append,
			})
			if !proto.Equal(got, tt.want) {
				t.Errorf("getObjectChecksums() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestGRPCWriter_MemoryAllocationPaths(t *testing.T) {
	tests := []struct {
		name         string
		chunkSize    int
		dataSize     int
		forceOneShot bool
		wantZeroCopy bool
	}{
		{
			name:         "OneShot_ZeroCopy_1MB",
			chunkSize:    0,
			dataSize:     1 * 1024 * 1024, // 1 MiB
			forceOneShot: true,
			wantZeroCopy: true,
		},
		{
			name:         "OneShot_ZeroCopy_10MB",
			chunkSize:    0,
			dataSize:     10 * 1024 * 1024, // 10 MiB
			forceOneShot: true,
			wantZeroCopy: true,
		},
		{
			name:         "Resumable_Buffering",
			chunkSize:    2 * 1024 * 1024, // 2 MiB
			dataSize:     1 * 1024 * 1024, // 1 MiB
			forceOneShot: false,
			wantZeroCopy: false,
		},
		{
			name:         "Resumable_ZeroCopy",
			chunkSize:    1 * 1024 * 1024, // 1 MiB
			dataSize:     2 * 1024 * 1024, // 2 MiB
			forceOneShot: false,
			wantZeroCopy: true,
		},
		{
			name:         "Resumable_Hybrid",
			chunkSize:    2 * 1024 * 1024, // 2 MiB
			dataSize:     3 * 1024 * 1024, // 3 MiB
			forceOneShot: false,
			wantZeroCopy: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, tt.dataSize)
			data[0] = 1
			data[tt.dataSize-1] = 2
			chunkSize := gRPCChunkSize(tt.chunkSize)
			mockSender := &mockSender{}
			w := &gRPCWriter{
				buf:           nil, // Allocated lazily on first buffered write.
				chunkSize:     chunkSize,
				forceOneShot:  tt.forceOneShot,
				writeQuantum:  maxPerMessageWriteSize,
				preRunCtx:     context.Background(),
				sendableUnits: 10,
				writesChan:    make(chan gRPCWriterCommand, 1),
				donec:         make(chan struct{}),
				streamSender:  mockSender,
				settings:      &settings{},
			}
			w.progress = func(int64) {}
			w.setObj = func(*ObjectAttrs) {}
			w.setSize = func(int64) {}

			go func() {
				w.writeLoop(context.Background())
				close(w.donec)
			}()

			if _, err := w.Write(data); err != nil {
				t.Fatalf("Write failed: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close failed: %v", err)
			}
			mockSender.wg.Wait()

			mockSender.mu.Lock()
			defer mockSender.mu.Unlock()

			reqs := filterDataRequests(mockSender.requests)
			if len(reqs) == 0 {
				t.Fatalf("Expected at least 1 data request, got 0")
			}

			// Verify memory address logic:
			// The last byte of the last request buffer should match the last byte of the input data for zero-copy.
			// For buffering/copying, the pointers must differ.
			idx := len(reqs) - 1
			bufIdx := len(reqs[idx].buf) - 1
			isZeroCopy := &reqs[idx].buf[bufIdx] == &data[tt.dataSize-1]
			if isZeroCopy != tt.wantZeroCopy {
				if tt.wantZeroCopy && tt.forceOneShot {
					t.Errorf("One-shot upload bypassed zero-copy path; data was unexpectedly copied")
				} else if !tt.wantZeroCopy && !tt.forceOneShot {
					t.Errorf("Resumable upload bypassed buffering path; data was unexpectedly zero-copied")
				} else if tt.wantZeroCopy && !tt.forceOneShot {
					t.Errorf("Resumable upload bypassed zero-copy path; data was unexpectedly copied")
				}
			}
		})
	}
}

type mockSender struct {
	mu               sync.Mutex
	requests         []gRPCBidiWriteRequest
	errResult        error
	wg               sync.WaitGroup // Waits for all async operations to complete.
	failOnData       bool
	respondToAllData bool
}

func (m *mockSender) connect(ctx context.Context, cs gRPCBufSenderChans, opts ...gax.CallOption) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		// Track active flush goroutines to prevent closing the channel prematurely.
		var completionWg sync.WaitGroup

		defer func() {
			completionWg.Wait()
			close(cs.completions)
		}()

		for req := range cs.requests {
			m.mu.Lock()
			m.requests = append(m.requests, req)
			failOnData := m.failOnData
			respondToAllData := m.respondToAllData
			m.mu.Unlock()

			if req.requestAck {
				select {
				case cs.requestAcks <- struct{}{}:
				case <-ctx.Done():
					return
				}
			}

			if failOnData && (req.flush || len(req.buf) > 0) {
				return
			}

			if req.flush || respondToAllData {
				completionWg.Add(1)
				// Send completions asynchronously to avoid blocking the request loop.
				go func(offset int64) {
					defer completionWg.Done()
					select {
					case cs.completions <- gRPCBidiWriteCompletion{
						flushOffset: offset,
					}:
					case <-ctx.Done():
					}
				}(req.offset + int64(len(req.buf)))
			}
		}
	}()
}

func (m *mockSender) err() error { return m.errResult }

// filterDataRequests returns only requests containing data, ignoring protocol overhead.
func filterDataRequests(reqs []gRPCBidiWriteRequest) []gRPCBidiWriteRequest {
	var dataReqs []gRPCBidiWriteRequest
	for _, r := range reqs {
		if len(r.buf) > 0 {
			dataReqs = append(dataReqs, r)
		}
	}
	return dataReqs
}

// Test the logic correctly handles the combination of io.EOF
// from Recv (recvErr) and a generic error from Send (sendErr).
func TestGRPCWriterErrorHandling(t *testing.T) {
	// As this is deeply embedded in the unexported types, we verify the logic
	// by simulating the exact error assignment sequence.
	tests := []struct {
		name      string
		recvErr   error
		sendErr   error
		wantError error
	}{
		{
			name:      "recvErr is io.EOF, sendErr is nil",
			recvErr:   io.EOF,
			sendErr:   nil,
			wantError: nil,
		},
		{
			name:      "recvErr is io.EOF, sendErr is an error",
			recvErr:   io.EOF,
			sendErr:   errors.New("send error"),
			wantError: errors.New("send error"), // Send error takes precedence.
		},
		{
			name:      "recvErr is an error, sendErr is nil",
			recvErr:   errors.New("recv error"),
			sendErr:   nil,
			wantError: errors.New("recv error"), // Recv error takes precedence.
		},
		{
			name:      "recvErr is an error, sendErr is an error",
			recvErr:   errors.New("recv error"),
			sendErr:   errors.New("send error"),
			wantError: errors.New("recv error"), // Recv error takes precedence.
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var streamErr error

			streamErr = pickStreamError(tt.recvErr, tt.sendErr)

			if tt.wantError == nil {
				if streamErr != nil {
					t.Errorf("got error %v, want nil", streamErr)
				}
			} else {
				if streamErr == nil || streamErr.Error() != tt.wantError.Error() {
					t.Errorf("got error %v, want %v", streamErr, tt.wantError)
				}
			}
		})
	}
}

// TestGRPCWriter_Deadlock simulates a deadlock scenario if Recv and Send channels
// were not isolated in gRPCOneshotBidiWriteBufferSender.
func TestGRPCWriter_Deadlock(t *testing.T) {
	// A timeout means a deadlock likely occurred.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sendDone := make(chan struct{})
	recvDone := make(chan struct{})

	requests := make(chan gRPCBidiWriteRequest)
	completions := make(chan gRPCBidiWriteCompletion)

	var sendErr error

	go func() {
		sendErr = func() error {
			for {
				select {
				case <-recvDone:
					return nil
				case r, ok := <-requests:
					if !ok {
						return nil
					}
					if r.requestAck {
						continue
					}
					// mimic send logic
					if r.finishWrite {
						return nil
					}
				}
			}
		}()
		close(sendDone)
	}()

	go func() {
		// Mimic recv loop that immediately exits.
		// If recvDone isn't checked by the sender loop, sending
		// requests could block forever if the consumer closes early.
		close(recvDone)
	}()

	// sendDone should be closed immediately.
	select {
	case <-sendDone:
		// Success, no deadlock.
	case <-ctx.Done():
		t.Fatal("deadlock detected: send loop did not exit after recvDone was closed")
	}

	if sendErr != nil {
		t.Errorf("expected no error, got %v", sendErr)
	}
	close(completions)
}

type instantFailSender struct {
	errResult error
}

func (i *instantFailSender) connect(ctx context.Context, cs gRPCBufSenderChans, opts ...gax.CallOption) {
	// Immediately close completions to simulate stream failure.
	// writeLoop will detect this instantly and return errResult.
	close(cs.completions)
}

func (i *instantFailSender) err() error {
	return i.errResult
}

func TestGRPCWriter_ChunkRetryDeadline_TimeoutEnforcedAcrossRetries(t *testing.T) {
	ctx := context.Background()
	deadline := 100 * time.Millisecond
	sender := &instantFailSender{errResult: errors.New("transient network error")}
	w := &gRPCWriter{
		chunkRetryDeadline: deadline,
		streamSender:       sender,
		settings:           &settings{},
		bufUnsentIdx:       100, // Makes isActive() == true.
		bufFlushedIdx:      0,
		buf:                make([]byte, 100),
		sendableUnits:      1,
		writeQuantum:       100,
		chunkSize:          100,
		writesChan:         make(chan gRPCWriterCommand, 1),
	}

	var err error
	for i := 0; i < 20; i++ {
		err = w.writeLoop(ctx)
		if err != nil && strings.Contains(err.Error(), "retry deadline") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err == nil || !strings.Contains(err.Error(), "retry deadline") {
		t.Fatalf("expected retry deadline error, got: %v", err)
	}
	if w.attempts < 2 {
		t.Errorf("expected multiple attempts before deadline was reached, got %d", w.attempts)
	}
}

func TestGRPCWriter_ChunkRetryDeadline_TimeoutResetOnProgress(t *testing.T) {
	ctx := context.Background()
	deadline := 200 * time.Millisecond
	sender := &instantFailSender{errResult: errors.New("transient network error")}
	w := &gRPCWriter{
		chunkRetryDeadline: deadline,
		streamSender:       sender,
		settings:           &settings{},
		bufBaseOffset:      0,
		bufUnsentIdx:       100,
		bufFlushedIdx:      0,
		buf:                make([]byte, 100),
		sendableUnits:      1,
		writeQuantum:       100,
		chunkSize:          100,
		writesChan:         make(chan gRPCWriterCommand, 1),
		setSize:            func(int64) {},
		progress:           func(int64) {},
	}

	// Attempt 1: Start the clock.
	_ = w.writeLoop(ctx)

	// Sleep to consume more than half the deadline.
	time.Sleep(120 * time.Millisecond)

	// Attempt 2: Clock should not be expired yet.
	err := w.writeLoop(ctx)
	if err != nil && strings.Contains(err.Error(), "retry deadline") {
		t.Fatalf("deadline reached too early: %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "transient network error") {
		t.Fatalf("expected transient network error, got: %v", err)
	}

	// Simulate forward progress by invoking handleCompletion.
	w.handleCompletion(gRPCBidiWriteCompletion{flushOffset: 50})

	// Sleep to consume another portion of the original deadline.
	// If the timer wasn't reset, the next writeLoop would fail since
	// 120ms + 120ms = 240ms > 200ms.
	time.Sleep(120 * time.Millisecond)

	// Attempt 3: Clock was reset, so this should NOT fail with deadline exceeded.
	err = w.writeLoop(ctx)
	if err != nil && strings.Contains(err.Error(), "retry deadline") {
		t.Fatalf("timer was not reset by forward progress, got deadline error: %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "transient network error") {
		t.Fatalf("expected transient network error, got: %v", err)
	}

	// Attempt 4: Wait for the reset timer to actually expire.
	time.Sleep(100 * time.Millisecond)
	err = w.writeLoop(ctx)
	if err == nil || !strings.Contains(err.Error(), "retry deadline") {
		t.Fatalf("expected retry deadline error after reset timer expired, got: %v", err)
	}
}

func TestGRPCWriter_ChunkRetryDeadline_TimeoutPausedOnIdle(t *testing.T) {
	ctx := context.Background()
	deadline := 100 * time.Millisecond
	sender := &instantFailSender{errResult: errors.New("transient network error")}
	w := &gRPCWriter{
		chunkRetryDeadline: deadline,
		streamSender:       sender,
		settings:           &settings{},
		bufUnsentIdx:       0, // Makes isActive() == false.
		bufFlushedIdx:      0,
		buf:                make([]byte, 0, 100),
		sendableUnits:      1,
		writeQuantum:       100,
		chunkSize:          100,
		writesChan:         make(chan gRPCWriterCommand, 1),
	}

	// Call writeLoop. Because isActive() is false, it should set abandonRetriesTime to zero.
	_ = w.writeLoop(ctx)

	if !w.abandonRetriesTime.IsZero() {
		t.Fatalf("expected timer to be zeroed when idle, but got: %v", w.abandonRetriesTime)
	}

	// Wait way past the deadline.
	time.Sleep(150 * time.Millisecond)

	// Next call should still not fail with deadline exceeded.
	err := w.writeLoop(ctx)
	if err != nil && strings.Contains(err.Error(), "retry deadline") {
		t.Fatalf("expected no deadline error when idle, got: %v", err)
	}
}

type checkTimerCmd struct {
	timerCh chan time.Time
}

func (c *checkTimerCmd) handle(w *gRPCWriter, cs gRPCWriterCommandHandleChans) error {
	c.timerCh <- w.abandonRetriesTime
	return nil
}

func TestGRPCWriter_ChunkRetryDeadline_TimerStartsOnlyWhenBufferFills(t *testing.T) {
	ctx := context.Background()
	deadline := 100 * time.Millisecond
	sender := &mockSender{errResult: errors.New("transient network error"), failOnData: true}

	w := &gRPCWriter{
		chunkRetryDeadline: deadline,
		streamSender:       sender,
		settings:           &settings{},
		bufUnsentIdx:       0,
		bufFlushedIdx:      0,
		buf:                make([]byte, 0, 100),
		sendableUnits:      1,
		writeQuantum:       100,
		chunkSize:          100,
		writesChan:         make(chan gRPCWriterCommand, 3),
		setSize:            func(int64) {},
		progress:           func(int64) {},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.writeLoop(ctx)
	}()

	for i := 0; i < 4; i++ {
		done := make(chan struct{})
		w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 20), done: done}
		<-done

		// Assert the timer is not started yet because the chunk size hasn't been reached.
		timerCh := make(chan time.Time)
		w.writesChan <- &checkTimerCmd{timerCh: timerCh}
		if abandonRetriesTime := <-timerCh; !abandonRetriesTime.IsZero() {
			t.Fatalf("expected timer to NOT be started before buffer fills, but it was %v after %d writes", abandonRetriesTime, i+1)
		}
	}

	done2 := make(chan struct{})
	w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 20), done: done2} // Fills buffer!

	err := <-errCh

	if err == nil || !strings.Contains(err.Error(), "transient network error") {
		t.Fatalf("expected transient network error when buffer fills and triggers send, got: %v", err)
	}
	if w.abandonRetriesTime.IsZero() {
		t.Fatalf("expected timer to be started when buffer fills and triggers send, but it was zero")
	}
}

func TestGRPCWriter_ChunkRetryDeadline_StaleTimerOnPartialBuffer(t *testing.T) {
	ctx := context.Background()
	deadline := 100 * time.Millisecond
	sender := &mockSender{errResult: errors.New("transient network error"), failOnData: false, respondToAllData: true}

	w := &gRPCWriter{
		chunkRetryDeadline: deadline,
		streamSender:       sender,
		settings:           &settings{},
		bufUnsentIdx:       0,
		bufFlushedIdx:      -1,
		buf:                make([]byte, 0, 1000),
		sendableUnits:      1,
		writeQuantum:       100,
		chunkSize:          1000,
		writesChan:         make(chan gRPCWriterCommand, 3),
		setSize:            func(int64) {},
		progress:           func(int64) {},
		setObj:             func(*ObjectAttrs) {},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.writeLoop(ctx)
	}()

	// Write 150 bytes (writeQuantum is 100).
	// 100 bytes will be sent and ACKed, 50 bytes will remain unsent in buf.
	done := make(chan struct{})
	w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 150), done: done}
	<-done

	// Wait a bit to ensure completion for the 100 bytes is processed.
	time.Sleep(50 * time.Millisecond)

	// Assert that timer is cleared when remaining unsent bytes (50) < writeQuantum (100).
	timerCh := make(chan time.Time)
	w.writesChan <- &checkTimerCmd{timerCh: timerCh}
	abandonRetriesTime := <-timerCh

	if !abandonRetriesTime.IsZero() {
		t.Fatalf("expected timer to be cleared when remaining unsent bytes < writeQuantum, but got: %v", abandonRetriesTime)
	}

	// Wait for old timer duration to pass.
	time.Sleep(150 * time.Millisecond)

	// Now fail on data requests.
	sender.mu.Lock()
	sender.failOnData = true
	sender.mu.Unlock()

	// Write 50 more bytes to complete the next quantum (100 total unsent bytes).
	done2 := make(chan struct{})
	w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 50), done: done2}

	err := <-errCh
	if err == nil || !strings.Contains(err.Error(), "transient network error") {
		t.Fatalf("expected transient network error, got: %v", err)
	}

	// Retry writeLoop. It should NOT instantly fail with retry deadline error due to a stale timer.
	err = w.writeLoop(ctx)
	if err != nil && strings.Contains(err.Error(), "retry deadline") {
		t.Fatalf("retry failed instantly due to stale timer: %v", err)
	}
}

// TestGRPCWriter_ChunkRetryDeadline_OversizedWriteStaleTimerOnClose tests that an oversized write (len(p) > chunkSize)
// that leaves unsent leftover bytes (< writeQuantum) in w.buf properly clears abandonRetriesTime when idle,
// so that a subsequent Close() operation after an idle delay does not fail with a stale retry deadline error.
func TestGRPCWriter_ChunkRetryDeadline_OversizedWriteStaleTimerOnClose(t *testing.T) {
	ctx := context.Background()
	deadline := 100 * time.Millisecond
	sender := &mockSender{errResult: errors.New("transient network error"), failOnData: false, respondToAllData: true}

	// chunkSize = 1000, writeQuantum = 350 (350 is NOT a factor of 1000; 1000 / 350 = 2 with remainder 300)
	w := &gRPCWriter{
		chunkRetryDeadline: deadline,
		streamSender:       sender,
		settings:           &settings{},
		bufUnsentIdx:       0,
		bufFlushedIdx:      -1,
		buf:                make([]byte, 0, 1000),
		sendableUnits:      3,
		writeQuantum:       350,
		chunkSize:          1000,
		lastSegmentStart:   700,
		writesChan:         make(chan gRPCWriterCommand, 3),
		setSize:            func(int64) {},
		progress:           func(int64) {},
		setObj:             func(*ObjectAttrs) {},
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- w.writeLoop(ctx)
	}()

	// 1. Write 1500 bytes (exceeds chunkSize of 1000: sends 350, 350, 300 tail chunk + 350 in buf; 150 unsent in buf).
	done := make(chan struct{})
	w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 1500), done: done}
	<-done
	time.Sleep(50 * time.Millisecond)

	timerCh := make(chan time.Time)
	w.writesChan <- &checkTimerCmd{timerCh: timerCh}
	if t1 := <-timerCh; !t1.IsZero() {
		t.Fatalf("expected timer to be zero after sending large write with 150 unsent bytes < 350 quantum, got: %v", t1)
	}

	// 2. Sleep past deadline while idle with 150 unsent bytes.
	time.Sleep(150 * time.Millisecond)

	// 3. Fail on data requests now.
	sender.mu.Lock()
	sender.failOnData = true
	sender.mu.Unlock()

	// 4. Send Close command (triggers tail send of remaining 150 bytes with finishWrite: true).
	w.writesChan <- &gRPCWriterCommandClose{err: nil}

	err := <-errCh
	if err == nil || !strings.Contains(err.Error(), "transient network error") {
		t.Fatalf("expected transient network error on Close, got: %v", err)
	}

	// 5. Retry writeLoop — must NOT fail with stale retry deadline error!
	err = w.writeLoop(ctx)
	if err == nil || !strings.Contains(err.Error(), "transient network error") {
		t.Fatalf("expected retry to fail with transient network error, got: %v", err)
	}
}

func TestGRPCWriter_ChunkRetryDeadline_SingleShotCloseError(t *testing.T) {
	ctx := context.Background()
	deadline := 100 * time.Millisecond
	sender := &mockSender{errResult: errors.New("transient network error"), failOnData: true}

	w := &gRPCWriter{
		chunkRetryDeadline: deadline,
		streamSender:       sender,
		settings:           &settings{},
		bufUnsentIdx:       0,
		bufFlushedIdx:      -1,
		buf:                make([]byte, 0, 1000),
		sendableUnits:      1,
		writeQuantum:       1000,
		chunkSize:          1000,
		writesChan:         make(chan gRPCWriterCommand, 3),
		setSize:            func(int64) {},
		progress:           func(int64) {},
		setObj:             func(*ObjectAttrs) {},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.writeLoop(ctx)
	}()

	// Write 50 bytes (fits in buffer, staged locally).
	done := make(chan struct{})
	w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 50), done: done}
	<-done

	// Send Close command.
	w.writesChan <- &gRPCWriterCommandClose{err: nil}

	err := <-errCh
	if err == nil || !strings.Contains(err.Error(), "transient network error") {
		t.Fatalf("expected transient network error on Close, got: %v", err)
	}

	// Retry writeLoop while failOnData is true. It must return transient network error, NOT a stale retry deadline error.
	err = w.writeLoop(ctx)
	if err == nil || !strings.Contains(err.Error(), "transient network error") {
		t.Fatalf("expected retry to fail with transient network error, got: %v", err)
	}
}

func TestGRPCWriter_ChunkRetryDeadline_PartialQuantumCloseStaleTimer(t *testing.T) {
	ctx := context.Background()
	deadline := 100 * time.Millisecond
	sender := &mockSender{errResult: errors.New("transient network error"), failOnData: false, respondToAllData: true}

	w := &gRPCWriter{
		chunkRetryDeadline: deadline,
		streamSender:       sender,
		settings:           &settings{},
		bufUnsentIdx:       0,
		bufFlushedIdx:      -1,
		buf:                make([]byte, 0, 1000),
		sendableUnits:      10,
		writeQuantum:       100,
		chunkSize:          1000,
		writesChan:         make(chan gRPCWriterCommand, 3),
		setSize:            func(int64) {},
		progress:           func(int64) {},
		setObj:             func(*ObjectAttrs) {},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.writeLoop(ctx)
	}()

	// 1. Write 150 bytes: 100 sent/ACKed, 50 unsent in buf.
	done := make(chan struct{})
	w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 150), done: done}
	<-done
	time.Sleep(50 * time.Millisecond)

	// Wait past deadline while idle with 50 unsent bytes.
	time.Sleep(120 * time.Millisecond)

	// Fail on data requests now.
	sender.mu.Lock()
	sender.failOnData = true
	sender.mu.Unlock()

	// 2. Send Close command (triggers tail send of remaining 50 bytes with finishWrite: true).
	w.writesChan <- &gRPCWriterCommandClose{err: nil}

	err := <-errCh
	if err == nil || !strings.Contains(err.Error(), "transient network error") {
		t.Fatalf("expected transient network error on Close tail, got: %v", err)
	}

	// 3. Retry writeLoop while failOnData is true. Must return transient network error, NOT a stale retry deadline error.
	err = w.writeLoop(ctx)
	if err == nil || !strings.Contains(err.Error(), "transient network error") {
		t.Fatalf("expected retry on Close tail to fail with transient network error, got: %v", err)
	}
}

type hangingConnectSender struct {
	errResult error
}

func (s *hangingConnectSender) connect(ctx context.Context, cs gRPCBufSenderChans, opts ...gax.CallOption) {
	<-ctx.Done()
	s.errResult = ctx.Err()
	close(cs.completions)
}

func (s *hangingConnectSender) err() error {
	return s.errResult
}

type hangingDataSender struct {
	errResult error
}

func (s *hangingDataSender) connect(ctx context.Context, cs gRPCBufSenderChans, opts ...gax.CallOption) {
	go func() {
		defer close(cs.completions)
		for {
			select {
			case <-ctx.Done():
				s.errResult = ctx.Err()
				return
			case r, ok := <-cs.requests:
				if !ok {
					return
				}
				if r.requestAck {
					select {
					case cs.requestAcks <- struct{}{}:
					case <-ctx.Done():
						s.errResult = ctx.Err()
						return
					}
				}
				// Intentionally do not send completions to simulate a stall on data.
			}
		}
	}()
}

func (s *hangingDataSender) err() error {
	return s.errResult
}

func TestGRPCWriter_ChunkTransferTimeout_Stage1_Stall(t *testing.T) {
	ctx := context.Background()
	timeout := 50 * time.Millisecond
	sender := &hangingConnectSender{}

	w := &gRPCWriter{
		chunkTransferTimeout: timeout,
		streamSender:         sender,
		settings:             &settings{},
		writesChan:           make(chan gRPCWriterCommand, 1),
	}

	start := time.Now()
	err := w.writeLoop(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected stall error, got nil")
	}
	if !errors.Is(err, errWriteStall) {
		t.Fatalf("expected errWriteStall, got: %v", err)
	}
	if !strings.Contains(err.Error(), "chunk transfer timed out") {
		t.Fatalf("unexpected error message: %v", err)
	}
	if elapsed < timeout {
		t.Fatalf("expected writeLoop to wait at least %v, but returned in %v", timeout, elapsed)
	}
}

func TestGRPCWriter_ChunkTransferTimeout_DataStall(t *testing.T) {
	ctx := context.Background()
	timeout := 50 * time.Millisecond
	sender := &hangingDataSender{}

	w := &gRPCWriter{
		chunkTransferTimeout: timeout,
		streamSender:         sender,
		settings:             &settings{},
		chunkSize:            100,
		writeQuantum:         100,
		buf:                  make([]byte, 100),
		bufBaseOffset:        0,
		bufUnsentIdx:         0,
		bufFlushedIdx:        -1,
		sendableUnits:        1,
		writesChan:           make(chan gRPCWriterCommand, 1),
	}

	start := time.Now()
	err := w.writeLoop(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected stall error, got nil")
	}
	if !errors.Is(err, errWriteStall) {
		t.Fatalf("expected errWriteStall, got: %v", err)
	}
	if elapsed < timeout {
		t.Fatalf("expected writeLoop to wait at least %v, but returned in %v", timeout, elapsed)
	}
}

func TestGRPCWriter_ChunkTransferTimeout_Telemetry(t *testing.T) {
	ctx := context.Background()
	timeout := 40 * time.Millisecond
	sender := &hangingConnectSender{}

	mr := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(mr))
	defer provider.Shutdown(ctx)

	cfg := storageConfig{
		enableOtelMetrics:      true,
		enableOtelDebugMetrics: true,
		meterProvider:          provider,
	}

	cm, _, err := initMetrics(ctx, "project-id", &cfg)
	if err != nil {
		t.Fatalf("initMetrics: %v", err)
	}

	state := &metricsState{metrics: cm}
	target := "storage.googleapis.com"
	state.target.Store(&target)
	ctxWithMetrics := contextWithMetricsState(ctx, state)

	w := &gRPCWriter{
		preRunCtx:            ctxWithMetrics,
		c:                    &grpcStorageClient{metrics: cm},
		chunkTransferTimeout: timeout,
		streamSender:         sender,
		settings:             &settings{},
		writesChan:           make(chan gRPCWriterCommand, 1),
	}

	err = w.writeLoop(ctx)
	if !errors.Is(err, errWriteStall) {
		t.Fatalf("expected errWriteStall, got: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := mr.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "gcp.storage.client.stall.duration" {
				found = true
				hist := m.Data.(metricdata.Histogram[float64])
				if len(hist.DataPoints) == 0 {
					t.Fatalf("expected data points, got 0")
				}
				dp := hist.DataPoints[0]
				if dp.Sum != timeout.Seconds() {
					t.Errorf("expected sum %v, got %v", timeout.Seconds(), dp.Sum)
				}
				if getHistAttr(dp, "rpc.method") != "BidiWriteObject" {
					t.Errorf("expected rpc.method BidiWriteObject, got %v", getHistAttr(dp, "rpc.method"))
				}
				if getHistAttr(dp, "rpc.system.name") != "grpc" {
					t.Errorf("expected rpc.system.name grpc, got %v", getHistAttr(dp, "rpc.system.name"))
				}
				if getHistAttr(dp, "server.address") != "storage.googleapis.com" {
					t.Errorf("expected server.address storage.googleapis.com, got %v", getHistAttr(dp, "server.address"))
				}
			}
		}
	}
	if !found {
		t.Errorf("metric gcp.storage.client.stall.duration not found")
	}
}

func TestGRPCWriter_ChunkTransferTimeout_ProgressAdvancesTimer(t *testing.T) {
	ctx := context.Background()
	timeout := 80 * time.Millisecond
	sender := &mockSender{respondToAllData: true}

	w := &gRPCWriter{
		chunkTransferTimeout: timeout,
		streamSender:         sender,
		settings:             &settings{},
		chunkSize:            100,
		writeQuantum:         50,
		buf:                  nil,
		bufBaseOffset:        0,
		bufUnsentIdx:         0,
		bufFlushedIdx:        -1,
		sendableUnits:        2,
		writesChan:           make(chan gRPCWriterCommand, 1),
		setSize:              func(int64) {},
		progress:             func(int64) {},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.writeLoop(ctx)
	}()

	// Perform 3 writes, each with a 30ms sleep (total ~90ms > 80ms timeout).
	// Because progress is acked on each write, the timer advances and doesn't stall.
	for i := 0; i < 3; i++ {
		done := make(chan struct{})
		w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 50), done: done}
		<-done
		time.Sleep(30 * time.Millisecond)
	}

	w.writesChan <- &gRPCWriterCommandClose{err: nil}
	err := <-errCh
	if err != nil {
		t.Fatalf("expected nil error on progress, got: %v", err)
	}
}

func TestGRPCWriter_ChunkTransferTimeout_PausedOnIdle(t *testing.T) {
	ctx := context.Background()
	timeout := 50 * time.Millisecond
	sender := &mockSender{respondToAllData: true}

	w := &gRPCWriter{
		chunkTransferTimeout: timeout,
		streamSender:         sender,
		settings:             &settings{},
		chunkSize:            100,
		writeQuantum:         50,
		buf:                  nil,
		bufBaseOffset:        0,
		bufUnsentIdx:         0,
		bufFlushedIdx:        -1,
		sendableUnits:        2,
		writesChan:           make(chan gRPCWriterCommand, 1),
		setSize:              func(int64) {},
		progress:             func(int64) {},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.writeLoop(ctx)
	}()

	// 1. Write 50 bytes and wait for it to be acked.
	done := make(chan struct{})
	w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 50), done: done}
	<-done
	time.Sleep(20 * time.Millisecond)

	// Writer is now idle waiting for caller's next Write().
	// Sleep 80ms (> 50ms timeout). Watchdog must be paused so no stall occurs.
	time.Sleep(80 * time.Millisecond)

	// 2. Write another 50 bytes. Watchdog resumes and acks.
	done2 := make(chan struct{})
	w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 50), done: done2}
	<-done2

	w.writesChan <- &gRPCWriterCommandClose{err: nil}
	err := <-errCh
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
}

func TestGRPCWriter_HasSession(t *testing.T) {
	// Resumable sender
	resumableSender := &gRPCResumableBidiWriteBufferSender{}
	wResumable := &gRPCWriter{streamSender: resumableSender}
	if wResumable.hasSession() {
		t.Errorf("resumable hasSession() without upid = true, want false")
	}
	resumableSender.upid = "upload-id-abc"
	if !wResumable.hasSession() {
		t.Errorf("resumable hasSession() with upid = false, want true")
	}

	// Append sender is excluded from session recovery
	wAppend := &gRPCWriter{append: true, appendGen: 12345}
	if wAppend.hasSession() {
		t.Errorf("append hasSession() = true, want false (appendable uploads excluded)")
	}

	// Oneshot sender
	oneshotSender := &gRPCOneshotBidiWriteBufferSender{}
	wOneshot := &gRPCWriter{streamSender: oneshotSender}
	if wOneshot.hasSession() {
		t.Errorf("oneshot hasSession() = true, want false")
	}
}

func TestGRPCWriter_RetryBudget_RecordProgress(t *testing.T) {
	w := &gRPCWriter{
		setSize:  func(int64) {},
		progress: func(int64) {},
	}
	w.retryBudget = newRetryBudget(w.hasSession)

	if w.lastPersistedOffset != 0 {
		t.Errorf("initial lastPersistedOffset = %v, want 0", w.lastPersistedOffset)
	}

	// Flush to offset 100.
	w.handleCompletion(gRPCBidiWriteCompletion{flushOffset: 100})
	if w.lastPersistedOffset != 100 {
		t.Errorf("lastPersistedOffset = %v, want 100", w.lastPersistedOffset)
	}
	if !w.retryBudget.checkAndResetProgress() {
		t.Errorf("expected progress reported on offset 100")
	}
	// Verify progress flag is cleared.
	if w.retryBudget.checkAndResetProgress() {
		t.Errorf("progress flag should be cleared after checkAndResetProgress")
	}

	// Duplicate completion to offset 100.
	w.handleCompletion(gRPCBidiWriteCompletion{flushOffset: 100})
	if w.retryBudget.checkAndResetProgress() {
		t.Errorf("duplicate offset 100 should not report progress")
	}

	// Flush to offset 250 (strictly advancing).
	w.handleCompletion(gRPCBidiWriteCompletion{flushOffset: 250})
	if w.lastPersistedOffset != 250 {
		t.Errorf("lastPersistedOffset = %v, want 250", w.lastPersistedOffset)
	}
	if !w.retryBudget.checkAndResetProgress() {
		t.Errorf("expected progress reported on offset 250")
	}
}

func TestGRPCWriter_WriterRetry_RetriesErrWriteStall(t *testing.T) {
	stallErr := fmt.Errorf("%w: chunk transfer timed out after 30s", errWriteStall)

	// Default ShouldRetry recognizes errWriteStall
	if !ShouldRetry(stallErr) {
		t.Fatalf("ShouldRetry should return true for errWriteStall")
	}

	// Custom shouldRetry that returns false for ordinary errors
	customRetry := &retryConfig{
		shouldRetry: func(err error, ctx *RetryContext) bool {
			return false
		},
	}

	// Wrap as done in OpenWriter
	origShouldRetry := customRetry.shouldRetry
	customRetry.shouldRetry = func(err error, retryCtx *RetryContext) bool {
		if errors.Is(err, errWriteStall) {
			return true
		}
		if origShouldRetry != nil {
			return origShouldRetry(err, retryCtx)
		}
		return ShouldRetry(err)
	}

	if !customRetry.runShouldRetry(stallErr, nil) {
		t.Fatalf("custom retry with wrapper should return true for errWriteStall")
	}
	if customRetry.runShouldRetry(errors.New("other non-retryable error"), nil) {
		t.Fatalf("custom retry with wrapper should return false for other errors")
	}
}

func runWriterWithRetry(w *gRPCWriter) error {
	if w.settings == nil {
		w.settings = &settings{}
	}
	if w.retryBudget == nil {
		w.retryBudget = newRetryBudget(w.hasSession)
	}
	if w.writesChan == nil {
		w.writesChan = make(chan gRPCWriterCommand, 5)
	}
	if w.setSize == nil {
		w.setSize = func(int64) {}
	}
	if w.progress == nil {
		w.progress = func(int64) {}
	}
	if w.setObj == nil {
		w.setObj = func(*ObjectAttrs) {}
	}
	if w.preRunCtx == nil {
		w.preRunCtx = context.Background()
	}

	writerRetry := w.settings.retry
	if writerRetry == nil {
		writerRetry = defaultRetry.clone()
	} else {
		writerRetry = writerRetry.clone()
	}
	writerRetry.maxRetryDuration = 0
	origShouldRetry := writerRetry.shouldRetry
	writerRetry.shouldRetry = func(err error, retryCtx *RetryContext) bool {
		if errors.Is(err, errWriteStall) {
			return true
		}
		if origShouldRetry != nil {
			return origShouldRetry(err, retryCtx)
		}
		return ShouldRetry(err)
	}
	return run(w.preRunCtx, func(ctx context.Context) error {
		w.lastErr = w.writeLoop(ctx)
		return w.lastErr
	}, writerRetry, w.settings.idempotent, withRetryBudget(w.retryBudget), withOperation("WriteObject"), withBucket(w.bucket), withObject("test-object"))
}

type gap1MockSender struct {
	mu           sync.Mutex
	attempts     int
	attemptHdrs  []string
	failAttempts map[int]error
	afterFlushFail map[int]struct {
		offset        int64
		err           error
		waitProgress  chan struct{}
		sendAckOffset int64
	}
	currentErr error
}

func (s *gap1MockSender) connect(ctx context.Context, cs gRPCBufSenderChans, opts ...gax.CallOption) {
	s.mu.Lock()
	s.attempts++
	attempt := s.attempts
	s.currentErr = nil
	headers := callctx.HeadersFromContext(ctx)
	if h, ok := headers[xGoogHeaderKey]; ok && len(h) > 0 {
		s.attemptHdrs = append(s.attemptHdrs, h[0])
	}
	connErr := s.failAttempts[attempt]
	afterFlush, hasAfterFlush := s.afterFlushFail[attempt]
	s.mu.Unlock()

	if connErr != nil {
		s.mu.Lock()
		s.currentErr = connErr
		s.mu.Unlock()
		close(cs.completions)
		return
	}

	go func() {
		defer close(cs.completions)
		for {
			select {
			case <-ctx.Done():
				s.mu.Lock()
				s.currentErr = ctx.Err()
				s.mu.Unlock()
				return
			case req, ok := <-cs.requests:
				if !ok {
					return
				}
				if req.requestAck {
					select {
					case cs.requestAcks <- struct{}{}:
					case <-ctx.Done():
						s.mu.Lock()
						s.currentErr = ctx.Err()
						s.mu.Unlock()
						return
					}
				}
				ackOffset := req.offset + int64(len(req.buf))
				if hasAfterFlush && afterFlush.sendAckOffset > 0 {
					ackOffset = afterFlush.sendAckOffset
				}
				if req.flush || len(req.buf) > 0 {
					select {
					case cs.completions <- gRPCBidiWriteCompletion{flushOffset: ackOffset}:
					case <-ctx.Done():
						s.mu.Lock()
						s.currentErr = ctx.Err()
						s.mu.Unlock()
						return
					}
				}
				if hasAfterFlush && (req.offset+int64(len(req.buf))) >= afterFlush.offset {
					if afterFlush.waitProgress != nil {
						select {
						case <-afterFlush.waitProgress:
						case <-ctx.Done():
							return
						}
					}
					s.mu.Lock()
					s.currentErr = afterFlush.err
					s.mu.Unlock()
					return
				}
			}
		}
	}()
}

func (s *gap1MockSender) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentErr
}

func (s *gap1MockSender) hasSession() bool {
	return true
}

func TestGRPCWriter_Gap1_PerChunkRetryReset(t *testing.T) {
	t.Run("StrictProgressResetsBudgetAndBackoff", func(t *testing.T) {
		ctx := context.Background()
		maxAttempts := 2
		retry := &retryConfig{
			maxAttempts: &maxAttempts,
			backoff: &gax.Backoff{
				Initial:    10 * time.Millisecond,
				Multiplier: 100,
				Max:        30 * time.Second,
			},
		}

		progressObserved1 := make(chan struct{})
		progressObserved2 := make(chan struct{})

		sender := &gap1MockSender{
			failAttempts: map[int]error{
				1: status.Error(codes.Unavailable, "attempt 1 connect failure"),
			},
			afterFlushFail: map[int]struct {
				offset        int64
				err           error
				waitProgress  chan struct{}
				sendAckOffset int64
			}{
				2: {
					offset:       100,
					err:          status.Error(codes.Unavailable, "attempt 2 stream broken after chunk 1"),
					waitProgress: progressObserved1,
				},
				3: {
					offset:       200,
					err:          status.Error(codes.Unavailable, "attempt 3 stream broken after chunk 2"),
					waitProgress: progressObserved2,
				},
			},
		}

		progressCh := make(chan int64, 10)
		w := &gRPCWriter{
			preRunCtx:     ctx,
			streamSender:  sender,
			settings:      &settings{retry: retry, idempotent: true},
			chunkSize:     100,
			writeQuantum:  100,
			sendableUnits: 2,
			writesChan:    make(chan gRPCWriterCommand, 5),
			setSize:       func(int64) {},
			progress: func(offset int64) {
				progressCh <- offset
			},
			setObj: func(*ObjectAttrs) {},
		}
		w.retryBudget = newRetryBudget(w.hasSession)

		// Start background goroutine mimicking sequential caller writes.
		go func() {
			// Chunk 1
			done1 := make(chan struct{})
			w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 100), done: done1}
			<-done1
			<-progressCh
			close(progressObserved1)

			// Chunk 2
			done2 := make(chan struct{})
			w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 100), done: done2}
			<-done2
			<-progressCh
			close(progressObserved2)

			// Chunk 3
			done3 := make(chan struct{})
			w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 100), done: done3}
			<-done3
			<-progressCh

			// Close
			w.writesChan <- &gRPCWriterCommandClose{err: nil}
		}()

		start := time.Now()
		err := runWriterWithRetry(w)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("expected success with per-chunk retry budget reset, got: %v", err)
		}

		sender.mu.Lock()
		totalAttempts := sender.attempts
		headers := append([]string{}, sender.attemptHdrs...)
		sender.mu.Unlock()

		if totalAttempts != 4 {
			t.Fatalf("expected 4 total attempts across chunks, got %d", totalAttempts)
		}

		// Verify backoff was reset to initial (10ms) after each chunk progress.
		// If backoff had compounded with Multiplier 100, attempt 3 or 4 sleep would be >= 1000ms.
		if elapsed > 500*time.Millisecond {
			t.Errorf("total elapsed time %v exceeded 500ms; backoff was likely not reset to initial", elapsed)
		}

		// Verify attempt headers:
		// Attempt 1: gccl-attempt-count/1
		// Attempt 2: gccl-attempt-count/2
		// Progress made on attempt 2 -> attempts reset to 1 -> Attempt 3: gccl-attempt-count/2
		// Progress made on attempt 3 -> attempts reset to 1 -> Attempt 4: gccl-attempt-count/2
		if len(headers) >= 4 {
			if !strings.Contains(headers[0], "gccl-attempt-count/1") {
				t.Errorf("attempt 1 header expected count 1, got %s", headers[0])
			}
			if !strings.Contains(headers[1], "gccl-attempt-count/2") {
				t.Errorf("attempt 2 header expected count 2, got %s", headers[1])
			}
			if !strings.Contains(headers[2], "gccl-attempt-count/2") {
				t.Errorf("attempt 3 header expected count 2 (after progress reset), got %s", headers[2])
			}
			if !strings.Contains(headers[3], "gccl-attempt-count/2") {
				t.Errorf("attempt 4 header expected count 2 (after progress reset), got %s", headers[3])
			}
		}
	})

	t.Run("NoResetOnNonAdvancingProgress", func(t *testing.T) {
		ctx := context.Background()
		maxAttempts := 2
		retry := &retryConfig{
			maxAttempts: &maxAttempts,
			backoff:     &gax.Backoff{Initial: 5 * time.Millisecond},
		}

		progressObserved1 := make(chan struct{})

		// Attempt 1 makes progress to offset 100 and fails.
		// Attempt 2 delivers duplicate offset 100 (sendAckOffset = 100, not advancing) and fails.
		sender := &gap1MockSender{
			afterFlushFail: map[int]struct {
				offset        int64
				err           error
				waitProgress  chan struct{}
				sendAckOffset int64
			}{
				1: {
					offset:       100,
					err:          status.Error(codes.Unavailable, "attempt 1 fail after offset 100"),
					waitProgress: progressObserved1,
				},
				2: {
					offset:        200,
					err:           status.Error(codes.Unavailable, "attempt 2 fail with duplicate offset 100"),
					sendAckOffset: 100, // Explicitly ack duplicate offset 100
				},
			},
		}

		progressCh := make(chan int64, 10)
		w := &gRPCWriter{
			preRunCtx:     ctx,
			streamSender:  sender,
			settings:      &settings{retry: retry, idempotent: true},
			chunkSize:     100,
			writeQuantum:  100,
			sendableUnits: 2,
			writesChan:    make(chan gRPCWriterCommand, 5),
			setSize:       func(int64) {},
			progress: func(offset int64) {
				progressCh <- offset
			},
			setObj: func(*ObjectAttrs) {},
		}
		w.retryBudget = newRetryBudget(w.hasSession)

		go func() {
			done1 := make(chan struct{})
			w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 100), done: done1}
			<-done1
			<-progressCh
			close(progressObserved1)

			done2 := make(chan struct{})
			w.writesChan <- &gRPCWriterCommandWrite{p: make([]byte, 100), done: done2}
			select {
			case <-done2:
			case <-time.After(200 * time.Millisecond):
			}
		}()

		err := runWriterWithRetry(w)
		if err == nil {
			t.Fatal("expected error after retry budget exhaustion, got nil")
		}
		if !strings.Contains(err.Error(), "retry failed after 2 attempts") {
			t.Fatalf("expected 'retry failed after 2 attempts', got: %v", err)
		}

		sender.mu.Lock()
		attempts := sender.attempts
		sender.mu.Unlock()

		if attempts != 2 {
			t.Fatalf("expected exactly 2 attempts before exhaustion, got %d", attempts)
		}
	})
}

type mockSessionSender struct {
	mu           sync.Mutex
	connectCalls int
	firstErr     error
	secondErr    error
	session      bool
}

func (s *mockSessionSender) connect(ctx context.Context, cs gRPCBufSenderChans, opts ...gax.CallOption) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connectCalls++
	close(cs.completions)
}

func (s *mockSessionSender) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.connectCalls == 1 {
		return s.firstErr
	}
	return s.secondErr
}

func (s *mockSessionSender) hasSession() bool {
	return s.session
}

func TestGRPCWriter_Gap2_SessionRecovery(t *testing.T) {
	transientErrors := []struct {
		name string
		err  error
	}{
		{
			name: "UNAVAILABLE",
			err:  status.Error(codes.Unavailable, "transient unavailable"),
		},
		{
			name: "DEADLINE_EXCEEDED",
			err:  status.Error(codes.DeadlineExceeded, "transient deadline exceeded"),
		},
		{
			name: "errWriteStall",
			err:  fmt.Errorf("%w: chunk transfer timed out after 30s", errWriteStall),
		},
	}

	t.Run("BeforeSession_NotRetried", func(t *testing.T) {
		for _, tc := range transientErrors {
			t.Run(tc.name, func(t *testing.T) {
				t.Run("MockSender_NoSession", func(t *testing.T) {
					sender := &mockSessionSender{
						firstErr:  tc.err,
						secondErr: nil,
						session:   false,
					}
					w := &gRPCWriter{
						streamSender: sender,
						settings: &settings{
							retry:      &retryConfig{backoff: &gax.Backoff{Initial: time.Millisecond}},
							idempotent: false, // Preconditions not specified
						},
					}

					err := runWriterWithRetry(w)
					if !errors.Is(err, tc.err) && err.Error() != tc.err.Error() {
						t.Fatalf("expected error %v, got: %v", tc.err, err)
					}

					sender.mu.Lock()
					calls := sender.connectCalls
					sender.mu.Unlock()

					if calls != 1 {
						t.Fatalf("expected exactly 1 attempt (no retry), got %d", calls)
					}
				})

				t.Run("AppendSender_NoSession", func(t *testing.T) {
					sender := &mockSessionSender{
						firstErr:  tc.err,
						secondErr: nil,
						session:   false,
					}
					w := &gRPCWriter{
						streamSender: sender,
						append:       true,
						appendGen:    -1, // No append generation established
						settings: &settings{
							retry:      &retryConfig{backoff: &gax.Backoff{Initial: time.Millisecond}},
							idempotent: false,
						},
					}

					err := runWriterWithRetry(w)
					if !errors.Is(err, tc.err) && err.Error() != tc.err.Error() {
						t.Fatalf("expected error %v, got: %v", tc.err, err)
					}

					sender.mu.Lock()
					calls := sender.connectCalls
					sender.mu.Unlock()

					if calls != 1 {
						t.Fatalf("expected exactly 1 attempt (no retry), got %d", calls)
					}
				})
			})
		}
	})

	t.Run("WithSession_Retried", func(t *testing.T) {
		for _, tc := range transientErrors {
			t.Run(tc.name, func(t *testing.T) {
				t.Run("MockSender_HasSession", func(t *testing.T) {
					sender := &mockSessionSender{
						firstErr:  tc.err,
						secondErr: nil,
						session:   true,
					}
					w := &gRPCWriter{
						streamSender: sender,
						settings: &settings{
							retry:      &retryConfig{backoff: &gax.Backoff{Initial: time.Millisecond}},
							idempotent: false, // Preconditions not specified, but session exists!
						},
					}

					err := runWriterWithRetry(w)
					if err != nil {
						t.Fatalf("expected successful retry with session, got error: %v", err)
					}

					sender.mu.Lock()
					calls := sender.connectCalls
					sender.mu.Unlock()

					if calls != 2 {
						t.Fatalf("expected exactly 2 attempts (retried after transient error), got %d", calls)
					}
				})

				t.Run("Append_NoSessionRecovery", func(t *testing.T) {
					sender := &mockSessionSender{
						firstErr:  tc.err,
						secondErr: nil,
						session:   false,
					}
					w := &gRPCWriter{
						streamSender: sender,
						append:       true,
						appendGen:    12345,
						settings: &settings{
							retry:      &retryConfig{backoff: &gax.Backoff{Initial: time.Millisecond}},
							idempotent: false,
						},
					}

					err := runWriterWithRetry(w)
					if err == nil {
						t.Fatalf("expected error because appendable uploads are excluded from session recovery, got nil")
					}

					sender.mu.Lock()
					calls := sender.connectCalls
					sender.mu.Unlock()

					if calls != 1 {
						t.Fatalf("expected exactly 1 attempt (no retry), got %d", calls)
					}
				})
			})
		}
	})

	t.Run("WithSession_NonRetryable_NotRetried", func(t *testing.T) {
		nonRetryableErrors := []error{
			status.Error(codes.InvalidArgument, "invalid argument"),
			status.Error(codes.NotFound, "not found"),
		}

		for _, nonRetryErr := range nonRetryableErrors {
			sender := &mockSessionSender{
				firstErr:  nonRetryErr,
				secondErr: nil,
				session:   true, // Session exists, but error is not retryable
			}
			w := &gRPCWriter{
				streamSender: sender,
				settings: &settings{
					retry:      &retryConfig{backoff: &gax.Backoff{Initial: time.Millisecond}},
					idempotent: false,
				},
			}

			err := runWriterWithRetry(w)
			if !errors.Is(err, nonRetryErr) && err.Error() != nonRetryErr.Error() {
				t.Fatalf("expected error %v, got: %v", nonRetryErr, err)
			}

			sender.mu.Lock()
			calls := sender.connectCalls
			sender.mu.Unlock()

			if calls != 1 {
				t.Fatalf("expected 1 attempt (non-retryable error not retried), got %d", calls)
			}
		}
	})

	t.Run("PreconditionsSpecified_RetriesBeforeSession", func(t *testing.T) {
		// When caller specified preconditions (idempotent == true), transient errors are retried even before session exists.
		sender := &mockSessionSender{
			firstErr:  status.Error(codes.Unavailable, "unavailable"),
			secondErr: nil,
			session:   false,
		}
		w := &gRPCWriter{
			streamSender: sender,
			settings: &settings{
				retry:      &retryConfig{backoff: &gax.Backoff{Initial: time.Millisecond}},
				idempotent: true, // Caller specified preconditions
			},
		}

		err := runWriterWithRetry(w)
		if err != nil {
			t.Fatalf("expected successful retry when idempotent == true, got: %v", err)
		}

		sender.mu.Lock()
		calls := sender.connectCalls
		sender.mu.Unlock()

		if calls != 2 {
			t.Fatalf("expected 2 attempts when idempotent == true, got %d", calls)
		}
	})
}

func TestGRPCWriter_ChunkTransferTimeout_IgnoredForAppend(t *testing.T) {
	c := &grpcStorageClient{settings: &settings{}}
	params := &openWriterParams{
		ctx:                  context.Background(),
		bucket:               "bucket",
		attrs:                &ObjectAttrs{Name: "object"},
		append:               true,
		appendGen:            -1,
		chunkTransferTimeout: 5 * time.Second,
		donec:                make(chan struct{}),
		setError:             func(error) {},
	}
	iw, err := c.OpenWriter(params)
	if err != nil {
		t.Fatalf("OpenWriter failed: %v", err)
	}
	w := iw.(*gRPCWriter)
	defer w.CloseWithError(errors.New("test done"))
	if w.chunkTransferTimeout != 0 {
		t.Errorf("expected chunkTransferTimeout == 0 for append upload, got %v", w.chunkTransferTimeout)
	}
}
