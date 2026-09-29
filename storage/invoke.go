// Copyright 2014 Google LLC
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
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/internal/version"
	sinternal "cloud.google.com/go/storage/internal"
	"github.com/google/uuid"
	gax "github.com/googleapis/gax-go/v2"
	"github.com/googleapis/gax-go/v2/callctx"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	defaultRetry  *retryConfig = &retryConfig{}
	errWriteStall              = errors.New("storage: write stall detected")
)
var xGoogDefaultHeader = fmt.Sprintf("gl-go/%s gccl/%s", version.Go(), sinternal.Version)

const (
	xGoogHeaderKey            = "x-goog-api-client"
	idempotencyHeaderKey      = "x-goog-gcs-idempotency-token"
	cookieHeaderKey           = "cookie"
	directpathCookieHeaderKey = "x-directpath-tracing-cookie"
)

var (
	cookieHeader = sync.OnceValue(func() string {
		return os.Getenv("GOOGLE_SDK_GO_TRACING_COOKIE")
	})
)

// runShouldRetry calls the configured shouldRetry function if it exists,
// otherwise it falls back to the default ShouldRetry function.
func (r *retryConfig) runShouldRetry(err error, retryCtx *RetryContext) bool {
	if r == nil || r.shouldRetry == nil {
		return ShouldRetry(err)
	}

	return r.shouldRetry(err, retryCtx)
}

// retryBudget tracks upload session status and progress for per-chunk retry budgets
// (Gap 1) and session recovery (Gap 2).
type retryBudget struct {
	mu            sync.Mutex
	hasSession    func() bool
	onProgress    func()
	lastPersisted int64
	progressMade  bool
}

// retryController is an alias for retryBudget for managing per-chunk retry budgets
// and session recovery.
type retryController = retryBudget

// newRetryBudget creates a new retryBudget with the provided hasSession callback.
func newRetryBudget(hasSession func() bool) *retryBudget {
	return &retryBudget{
		hasSession: hasSession,
	}
}

// recordProgress signals that GCS persisted offset has updated.
// If persistedOffset strictly advances beyond previous known offset,
// it marks progress as made and invokes onProgress if configured.
// It returns true if progress was made.
func (b *retryBudget) recordProgress(persistedOffset int64) bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	var onProg func()
	progress := false
	if persistedOffset > b.lastPersisted {
		b.lastPersisted = persistedOffset
		b.progressMade = true
		onProg = b.onProgress
		progress = true
	}
	b.mu.Unlock()
	if onProg != nil {
		onProg()
	}
	return progress
}

// reportProgress signals that progress has been made regardless of offset,
// marking the retry budget for reset on the next retry evaluation.
func (b *retryBudget) reportProgress() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.progressMade = true
	onProg := b.onProgress
	b.mu.Unlock()
	if onProg != nil {
		onProg()
	}
}

// checkAndResetProgress returns true if progress was reported since the last check,
// and resets the progress flag.
func (b *retryBudget) checkAndResetProgress() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	p := b.progressMade
	b.progressMade = false
	return p
}

// hasActiveSession returns whether an upload session currently exists.
func (b *retryBudget) hasActiveSession() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	fn := b.hasSession
	b.mu.Unlock()
	if fn != nil {
		return fn()
	}
	return false
}

// lastPersistedOffset returns the currently recorded last persisted offset.
func (b *retryBudget) lastPersistedOffset() int64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastPersisted
}

// setLastPersistedOffset sets the last persisted offset.
func (b *retryBudget) setLastPersistedOffset(offset int64) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastPersisted = offset
}

// runOptions holds optional metadata for retry contexts.
type runOptions struct {
	operation        string
	bucket           string
	object           string
	retryBudget      *retryBudget
	progressCallback func()
}

// runOption configures optional metadata for retry contexts.
type runOption func(*runOptions)

// withOperation specifies the operation name for retry context.
func withOperation(op string) runOption {
	return func(o *runOptions) { o.operation = op }
}

// withBucket specifies the bucket name for retry context.
func withBucket(bucket string) runOption {
	return func(o *runOptions) { o.bucket = bucket }
}

// withObject specifies the object name for retry context.
func withObject(object string) runOption {
	return func(o *runOptions) { o.object = object }
}

// withRetryBudget specifies a retry budget for per-chunk retry budget and session recovery.
func withRetryBudget(b *retryBudget) runOption {
	return func(o *runOptions) { o.retryBudget = b }
}

// withRetryController specifies a retry controller for per-chunk retry budget and session recovery.
func withRetryController(c *retryController) runOption {
	return func(o *runOptions) { o.retryBudget = c }
}

// withProgressCallback specifies a callback function to be called when progress is made.
func withProgressCallback(cb func()) runOption {
	return func(o *runOptions) {
		o.progressCallback = cb
	}
}

// run determines whether a retry is necessary based on the config and
// idempotency information. It then calls the function with or without retries
// as appropriate, using the configured settings.
// TODO: consider replacing the functional option (runOption) pattern with a
// hardcoded struct based approach if parameter related changes requires for all
// the callers. Ref: http://shortn/_ciY2iWLh2J
func run(ctx context.Context, call func(ctx context.Context) error, retry *retryConfig, isIdempotent bool, opts ...runOption) error {
	options := &runOptions{}
	for _, opt := range opts {
		opt(options)
	}
	if options.progressCallback != nil && options.retryBudget != nil && options.retryBudget.onProgress == nil {
		options.retryBudget.onProgress = options.progressCallback
	}

	attempts := 1
	invocationID := uuid.New().String()
	retryCtx := &RetryContext{
		Attempt:      attempts,
		InvocationID: invocationID,
		Operation:    options.operation,
		Bucket:       options.bucket,
		Object:       options.object,
	}

	if retry == nil {
		retry = defaultRetry
	}
	if retry.policy == RetryNever {
		ctxWithHeaders := setInvocationHeaders(ctx, invocationID, attempts)
		return call(ctxWithHeaders)
	}
	if retry.policy == RetryIdempotent && !isIdempotent && options.retryBudget == nil {
		ctxWithHeaders := setInvocationHeaders(ctx, invocationID, attempts)
		return call(ctxWithHeaders)
	}

	initialBo := gax.Backoff{}
	if retry.backoff != nil {
		initialBo.Multiplier = retry.backoff.Multiplier
		initialBo.Initial = retry.backoff.Initial
		initialBo.Max = retry.backoff.Max
	}
	bo := initialBo

	var quitAfterTimer *time.Timer
	if retry.maxRetryDuration != 0 {
		quitAfterTimer = time.NewTimer(retry.maxRetryDuration)
		defer quitAfterTimer.Stop()
	}

	var lastErr error
	for {
		if retry.maxRetryDuration != 0 {
			select {
			case <-quitAfterTimer.C:
				if lastErr == nil {
					return fmt.Errorf("storage: request not sent, choose a larger value for the retry deadline (currently set to %s)", retry.maxRetryDuration)
				}
				return fmt.Errorf("storage: retry deadline of %s reached after %v attempts; last error: %w", retry.maxRetryDuration, attempts, lastErr)
			default:
			}
		}

		ctxWithHeaders := setInvocationHeaders(ctx, invocationID, attempts)
		lastErr = call(ctxWithHeaders)
		if lastErr == nil {
			return nil
		}

		// Gap 1 (Per-Chunk Retry Budget):
		// When progress is reported (GCS persisted offset strictly advances),
		// reset attempts to 1 and backoff to initial so the next failure
		// backs off from the initial duration instead of compounding across chunks.
		if options.retryBudget != nil && options.retryBudget.checkAndResetProgress() {
			attempts = 1
			bo = initialBo
		}

		if retry.maxAttempts != nil && attempts >= *retry.maxAttempts {
			return fmt.Errorf("storage: retry failed after %v attempts; last error: %w", *retry.maxAttempts, lastErr)
		}

		retryCtx.Attempt = attempts
		retryable := retry.runShouldRetry(lastErr, retryCtx)

		// Gap 2 (Session Recovery):
		// When caller did not specify preconditions (!isIdempotent) and retry policy is RetryIdempotent,
		// retries are allowed only if an upload session exists (hasActiveSession() is true).
		// If hasActiveSession() is false, do not retry and return lastErr immediately.
		if !isIdempotent && retry.policy == RetryIdempotent {
			if options.retryBudget == nil || !options.retryBudget.hasActiveSession() {
				retryable = false
			}
		}

		// Explicitly check context cancellation so that we can distinguish between a
		// DEADLINE_EXCEEDED error from the server and a user-set context deadline.
		// Unfortunately gRPC will codes.DeadlineExceeded (which may be retryable if it's
		// sent by the server) in both cases.
		if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) || errors.Is(ctxErr, context.DeadlineExceeded) {
			retryable = false
		}

		if !retryable {
			return lastErr
		}

		attempts++
		p := bo.Pause()
		if ctxErr := gax.Sleep(ctx, p); ctxErr != nil {
			if lastErr != nil {
				return wrappedCallErr{ctxErr: ctxErr, wrappedErr: lastErr}
			}
			return ctxErr
		}
	}
}

type wrappedCallErr struct {
	ctxErr     error
	wrappedErr error
}

func (e wrappedCallErr) Error() string {
	return fmt.Sprintf("retry failed with %v; last error: %v", e.ctxErr, e.wrappedErr)
}

func (e wrappedCallErr) Unwrap() error {
	return e.wrappedErr
}

func (e wrappedCallErr) Is(err error) bool {
	return e.ctxErr == err || e.wrappedErr == err
}

// Sets invocation ID headers on the context which will be propagated as
// headers in the call to the service (for both gRPC and HTTP).
func setInvocationHeaders(ctx context.Context, invocationID string, attempts int) context.Context {
	invocationHeader := fmt.Sprintf("gccl-invocation-id/%v gccl-attempt-count/%v", invocationID, attempts)
	xGoogHeader := strings.Join([]string{invocationHeader, xGoogDefaultHeader}, " ")

	ctx = callctx.SetHeaders(ctx, xGoogHeaderKey, xGoogHeader)
	ctx = callctx.SetHeaders(ctx, idempotencyHeaderKey, invocationID)

	if c := cookieHeader(); c != "" {
		ctx = callctx.SetHeaders(ctx, cookieHeaderKey, c)
		ctx = callctx.SetHeaders(ctx, directpathCookieHeaderKey, c)
	}

	return ctx
}

// ShouldRetry returns true if an error is retryable, based on best practice
// guidance from GCS. See
// https://cloud.google.com/storage/docs/retry-strategy#go for more information
// on what errors are considered retryable.
//
// If you would like to customize retryable errors, use the WithErrorFunc to
// supply a RetryOption to your library calls. For example, to retry additional
// errors, you can write a custom func that wraps ShouldRetry and also specifies
// additional errors that should return true.
func ShouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errWriteStall) {
		return true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}

	switch e := err.(type) {
	case *googleapi.Error:
		// Retry on 408, 429, and 5xx, according to
		// https://cloud.google.com/storage/docs/exponential-backoff.
		return e.Code == 408 || e.Code == 429 || (e.Code >= 500 && e.Code < 600)
	case *net.OpError, *url.Error:
		// Retry socket-level errors ECONNREFUSED and ECONNRESET (from syscall)
		// and transport-level errors like server closed idle connections.
		// Unfortunately the error type is unexported, so we resort to string
		// matching.
		retriable := []string{"connection refused", "connection reset", "broken pipe", "client connection lost", "server closed idle connection"}
		for _, s := range retriable {
			if strings.Contains(e.Error(), s) {
				return true
			}
		}
		// TODO: remove when https://github.com/golang/go/issues/53472 is resolved.
		// We don't want to retry io.EOF errors, since these can indicate normal
		// functioning terminations such as internally in the case of Reader and
		// externally in the case of iterator methods. However, the linked bug
		// requires us to retry the EOFs that it causes, which should be wrapped
		// in net or url errors.
		if errors.Is(err, io.EOF) {
			return true
		}
	case *net.DNSError:
		if e.IsTemporary {
			return true
		}
	case interface{ Temporary() bool }:
		if e.Temporary() {
			return true
		}
	}
	// UNAVAILABLE, RESOURCE_EXHAUSTED, INTERNAL, and DEADLINE_EXCEEDED codes are all retryable for gRPC.
	if st, ok := status.FromError(err); ok {
		if code := st.Code(); code == codes.Unavailable || code == codes.ResourceExhausted || code == codes.Internal || code == codes.DeadlineExceeded {
			return true
		}
	}
	// Unwrap is only supported in go1.13.x+
	if e, ok := err.(interface{ Unwrap() error }); ok {
		return ShouldRetry(e.Unwrap())
	}
	return false
}

func isError(err error, httpErrorCode int, grpcErrorCode codes.Code) bool {
	var e *googleapi.Error
	if errors.As(err, &e) {
		if e.Code == httpErrorCode {
			return true
		}
	}
	if s, ok := status.FromError(err); ok {
		if s.Code() == grpcErrorCode {
			return true
		}
	}
	return false
}
