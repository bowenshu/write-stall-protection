// Copyright 2020 Google LLC
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
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/googleapis/gax-go/v2"
	"github.com/googleapis/gax-go/v2/callctx"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInvoke(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// Time-based tests are flaky. We just make sure that invoke eventually
	// returns with the right error.

	for _, test := range []struct {
		desc              string
		count             int   // Number of times to return retryable error.
		initialErr        error // Error to return initially.
		finalErr          error // Error to return after count returns of retryCode.
		retry             *retryConfig
		isIdempotentValue bool
		expectFinalErr    bool
		wantAttempts      *int
	}{
		{
			desc:              "test fn never returns initial error with count=0",
			count:             0,
			initialErr:        &googleapi.Error{Code: 0}, //non-retryable
			finalErr:          nil,
			isIdempotentValue: true,
			expectFinalErr:    true,
			wantAttempts:      intPointer(1),
		},
		{
			desc:              "non-retryable error is returned without retrying",
			count:             1,
			initialErr:        &googleapi.Error{Code: 0},
			finalErr:          nil,
			isIdempotentValue: true,
			expectFinalErr:    false,
			wantAttempts:      intPointer(1),
		},
		{
			desc:              "retryable error is retried",
			count:             1,
			initialErr:        &googleapi.Error{Code: 429},
			finalErr:          nil,
			isIdempotentValue: true,
			expectFinalErr:    true,
			wantAttempts:      intPointer(2),
		},
		{
			desc:              "retryable gRPC error is retried",
			count:             1,
			initialErr:        status.Error(codes.ResourceExhausted, "rate limit"),
			finalErr:          nil,
			isIdempotentValue: true,
			expectFinalErr:    true,
			wantAttempts:      intPointer(2),
		},
		{
			desc:              "returns non-retryable error after retryable error",
			count:             1,
			initialErr:        &googleapi.Error{Code: 429},
			finalErr:          errors.New("bar"),
			isIdempotentValue: true,
			expectFinalErr:    true,
			wantAttempts:      intPointer(2),
		},
		{
			desc:              "retryable 5xx error is retried",
			count:             2,
			initialErr:        &googleapi.Error{Code: 518},
			finalErr:          nil,
			isIdempotentValue: true,
			expectFinalErr:    true,
			wantAttempts:      intPointer(3),
		},
		{
			desc:              "retriable error not retried when non-idempotent",
			count:             2,
			initialErr:        &googleapi.Error{Code: 599},
			finalErr:          nil,
			isIdempotentValue: false,
			expectFinalErr:    false,
			wantAttempts:      intPointer(1),
		},
		{
			desc:              "non-idempotent retriable error retried when policy is RetryAlways",
			count:             2,
			initialErr:        &googleapi.Error{Code: 500},
			finalErr:          nil,
			isIdempotentValue: false,
			retry:             &retryConfig{policy: RetryAlways},
			expectFinalErr:    true,
			wantAttempts:      intPointer(3),
		},
		{
			desc:              "retriable error not retried when policy is RetryNever",
			count:             2,
			initialErr:        &url.Error{Op: "blah", URL: "blah", Err: errors.New("connection refused")},
			finalErr:          nil,
			isIdempotentValue: true,
			retry:             &retryConfig{policy: RetryNever},
			expectFinalErr:    false,
			wantAttempts:      intPointer(1),
		},
		{
			desc:              "non-retriable error not retried when policy is RetryAlways",
			count:             2,
			initialErr:        fmt.Errorf("non-retriable error: %w", &googleapi.Error{Code: 400}),
			finalErr:          nil,
			isIdempotentValue: true,
			retry:             &retryConfig{policy: RetryAlways},
			expectFinalErr:    false,
			wantAttempts:      intPointer(1),
		},
		{
			desc:              "non-retriable error retried with custom fn",
			count:             2,
			initialErr:        io.ErrNoProgress,
			finalErr:          nil,
			isIdempotentValue: true,
			retry: &retryConfig{
				shouldRetry: func(err error, retryCtx *RetryContext) bool {
					return err == io.ErrNoProgress
				},
			},
			expectFinalErr: true,
			wantAttempts:   intPointer(3),
		},
		{
			desc:              "retriable error not retried with custom fn",
			count:             2,
			initialErr:        io.ErrUnexpectedEOF,
			finalErr:          nil,
			isIdempotentValue: true,
			retry: &retryConfig{
				shouldRetry: func(err error, retryCtx *RetryContext) bool {
					return err == io.ErrNoProgress
				},
			},
			expectFinalErr: false,
			wantAttempts:   intPointer(1),
		},
		{
			desc:              "error not retried when policy is RetryNever despite custom fn",
			count:             2,
			initialErr:        io.ErrUnexpectedEOF,
			finalErr:          nil,
			isIdempotentValue: true,
			retry: &retryConfig{
				shouldRetry: func(err error, retryCtx *RetryContext) bool {
					return err == io.ErrUnexpectedEOF
				},
				policy: RetryNever,
			},
			expectFinalErr: false,
			wantAttempts:   intPointer(1),
		},
		{
			desc:              "non-idempotent retriable error retried when policy is RetryAlways till maxAttempts",
			count:             4,
			initialErr:        &googleapi.Error{Code: 500},
			finalErr:          nil,
			isIdempotentValue: false,
			retry:             &retryConfig{policy: RetryAlways, maxAttempts: intPointer(2)},
			expectFinalErr:    false,
			wantAttempts:      intPointer(2),
		},
		{
			desc:              "non-idempotent retriable error not retried when policy is RetryNever with maxAttempts set",
			count:             4,
			initialErr:        &googleapi.Error{Code: 500},
			finalErr:          nil,
			isIdempotentValue: false,
			retry:             &retryConfig{policy: RetryNever, maxAttempts: intPointer(2)},
			expectFinalErr:    false,
			wantAttempts:      intPointer(1),
		},
		{
			desc:              "non-retriable error retried with custom fn till maxAttempts",
			count:             4,
			initialErr:        io.ErrNoProgress,
			finalErr:          nil,
			isIdempotentValue: true,
			retry: &retryConfig{
				shouldRetry: func(err error, retryCtx *RetryContext) bool {
					return err == io.ErrNoProgress
				},
				maxAttempts: intPointer(2),
			},
			expectFinalErr: false,
			wantAttempts:   intPointer(2),
		},
		{
			desc:              "non-idempotent retriable error retried when policy is RetryAlways till maxAttempts where count equals to maxAttempts-1",
			count:             3,
			initialErr:        &googleapi.Error{Code: 500},
			finalErr:          nil,
			isIdempotentValue: false,
			retry:             &retryConfig{policy: RetryAlways, maxAttempts: intPointer(4)},
			expectFinalErr:    true,
			wantAttempts:      intPointer(4),
		},
		{
			desc:              "non-idempotent retriable error retried when policy is RetryAlways till maxAttempts where count equals to maxAttempts",
			count:             4,
			initialErr:        &googleapi.Error{Code: 500},
			finalErr:          nil,
			isIdempotentValue: true,
			retry:             &retryConfig{policy: RetryAlways, maxAttempts: intPointer(4)},
			expectFinalErr:    false,
			wantAttempts:      intPointer(4),
		},
		{
			desc:              "non-idempotent retriable error not retried when policy is RetryAlways with maxAttempts equals to zero",
			count:             4,
			initialErr:        &googleapi.Error{Code: 500},
			finalErr:          nil,
			isIdempotentValue: true,
			retry:             &retryConfig{maxAttempts: intPointer(0), policy: RetryAlways},
			expectFinalErr:    false,
			wantAttempts:      intPointer(1),
		},
		{
			desc:              "retry deadline stops retries",
			count:             20,
			initialErr:        &googleapi.Error{Code: 500},
			finalErr:          nil,
			isIdempotentValue: true,
			retry:             &retryConfig{policy: RetryAlways, maxRetryDuration: time.Second / 2},
			expectFinalErr:    false,
		},
		{
			desc:              "retry deadline not reached",
			count:             4,
			initialErr:        &googleapi.Error{Code: 500},
			finalErr:          nil,
			isIdempotentValue: true,
			retry:             &retryConfig{policy: RetryAlways, maxRetryDuration: time.Second / 2},
			expectFinalErr:    true,
			wantAttempts:      intPointer(5),
		},
		{
			desc:              "maxAttempts reached before maxRetryDuration",
			count:             10,
			initialErr:        &googleapi.Error{Code: 500},
			finalErr:          nil,
			isIdempotentValue: true,
			retry:             &retryConfig{policy: RetryAlways, maxAttempts: intPointer(3), maxRetryDuration: time.Second * 10},
			expectFinalErr:    false,
			wantAttempts:      intPointer(3),
		},
		{
			desc:              "maxRetryDuration reached before maxAttempts",
			count:             1000,
			initialErr:        &googleapi.Error{Code: 500},
			finalErr:          nil,
			isIdempotentValue: true,
			retry:             &retryConfig{policy: RetryAlways, maxAttempts: intPointer(1005), maxRetryDuration: time.Millisecond * 10},
			expectFinalErr:    false,
		},
		{
			desc:              "maxRetryDuration set to 0 allows infinite retries",
			count:             5,
			initialErr:        &googleapi.Error{Code: 500},
			finalErr:          nil,
			isIdempotentValue: true,
			retry:             &retryConfig{policy: RetryAlways, maxRetryDuration: 0},
			expectFinalErr:    true,
			wantAttempts:      intPointer(6),
		},
	} {
		t.Run(test.desc, func(s *testing.T) {
			counter := 0
			var initialClientHeader, initialIdempotencyHeader string
			var gotClientHeader, gotIdempotencyHeader string
			call := func(ctx context.Context) error {
				if counter == 0 {
					headers := callctx.HeadersFromContext(ctx)
					initialClientHeader = headers["x-goog-api-client"][0]
					initialIdempotencyHeader = headers["x-goog-gcs-idempotency-token"][0]
				}
				counter++
				headers := callctx.HeadersFromContext(ctx)
				gotClientHeader = headers["x-goog-api-client"][0]
				gotIdempotencyHeader = headers["x-goog-gcs-idempotency-token"][0]
				if counter <= test.count {
					return test.initialErr
				}
				return test.finalErr
			}
			// Use a short backoff to speed up the test.
			if test.retry == nil {
				test.retry = defaultRetry.clone()
			}
			test.retry.backoff = &gax.Backoff{Initial: time.Millisecond}
			got := run(ctx, call, test.retry, test.isIdempotentValue)
			if test.expectFinalErr && !errors.Is(got, test.finalErr) {
				s.Errorf("got %v, want %v", got, test.finalErr)
			} else if !test.expectFinalErr && !errors.Is(got, test.initialErr) {
				s.Errorf("got %v, want %v", got, test.initialErr)
			}

			if test.wantAttempts != nil {
				wantClientHeader := strings.ReplaceAll(initialClientHeader, "gccl-attempt-count/1", fmt.Sprintf("gccl-attempt-count/%v", *test.wantAttempts))
				if gotClientHeader != wantClientHeader {
					t.Errorf("case %q, retry header:\ngot %v\nwant %v", test.desc, gotClientHeader, wantClientHeader)
				}
			}

			wantClientHeaderFormat := "gccl-invocation-id/.{36} gccl-attempt-count/[0-9]+ gl-go/.* gccl/"
			match, err := regexp.MatchString(wantClientHeaderFormat, gotClientHeader)
			if err != nil {
				s.Fatalf("compiling regexp: %v", err)
			}
			if !match {
				s.Errorf("X-Goog-Api-Client header has wrong format\ngot %v\nwant regex matching %v", gotClientHeader, wantClientHeaderFormat)
			}
			if gotIdempotencyHeader != initialIdempotencyHeader {
				t.Errorf("case %q, idempotency header:\ngot %v\nwant %v", test.desc, gotIdempotencyHeader, initialIdempotencyHeader)
			}
		})
	}
}

type fakeApiaryRequest struct {
	header http.Header
}

func (f *fakeApiaryRequest) Header() http.Header {
	return f.header
}

func TestShouldRetry(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		desc        string
		inputErr    error
		shouldRetry bool
	}{
		{
			desc:        "googleapi.Error{Code: 0}",
			inputErr:    &googleapi.Error{Code: 0},
			shouldRetry: false,
		},
		{
			desc:        "googleapi.Error{Code: 429}",
			inputErr:    &googleapi.Error{Code: 429},
			shouldRetry: true,
		},
		{
			desc:        "errors.New(foo)",
			inputErr:    errors.New("foo"),
			shouldRetry: false,
		},
		{
			desc:        "googleapi.Error{Code: 518}",
			inputErr:    &googleapi.Error{Code: 518},
			shouldRetry: true,
		},
		{
			desc:        "googleapi.Error{Code: 599}",
			inputErr:    &googleapi.Error{Code: 599},
			shouldRetry: true,
		},
		{
			desc:        "googleapi.Error{Code: 428}",
			inputErr:    &googleapi.Error{Code: 428},
			shouldRetry: false,
		},
		{
			desc:        "googleapi.Error{Code: 518}",
			inputErr:    &googleapi.Error{Code: 518},
			shouldRetry: true,
		},
		{
			desc:        "url.Error{Err: errors.New(\"connection refused\")}",
			inputErr:    &url.Error{Op: "blah", URL: "blah", Err: errors.New("connection refused")},
			shouldRetry: true,
		},
		{
			desc:        "net.OpError{Err: errors.New(\"connection reset by peer\")}",
			inputErr:    &net.OpError{Op: "blah", Net: "tcp", Err: errors.New("connection reset by peer")},
			shouldRetry: true,
		},
		{
			desc:        "io.ErrUnexpectedEOF",
			inputErr:    io.ErrUnexpectedEOF,
			shouldRetry: true,
		},
		{
			desc:        "wrapped retryable error",
			inputErr:    fmt.Errorf("Test unwrapping of a temporary error: %w", &googleapi.Error{Code: 500}),
			shouldRetry: true,
		},
		{
			desc:        "wrapped non-retryable error",
			inputErr:    fmt.Errorf("Test unwrapping of a non-retriable error: %w", &googleapi.Error{Code: 400}),
			shouldRetry: false,
		},
		{
			desc:        "googleapi.Error{Code: 400}",
			inputErr:    &googleapi.Error{Code: 400},
			shouldRetry: false,
		},
		{
			desc:        "googleapi.Error{Code: 408}",
			inputErr:    &googleapi.Error{Code: 408},
			shouldRetry: true,
		},
		{
			desc:        "retryable gRPC error",
			inputErr:    status.Error(codes.Unavailable, "retryable gRPC error"),
			shouldRetry: true,
		},
		{
			desc:        "non-retryable gRPC error",
			inputErr:    status.Error(codes.PermissionDenied, "non-retryable gRPC error"),
			shouldRetry: false,
		},
		{
			desc:        "wrapped net.ErrClosed",
			inputErr:    &net.OpError{Err: net.ErrClosed},
			shouldRetry: true,
		},
		{
			desc:        "nil error",
			inputErr:    nil,
			shouldRetry: false,
		},
		{
			desc:        "http2: client connection lost",
			inputErr:    &url.Error{Op: "blah", URL: "blah", Err: errors.New("http2: client connection lost")},
			shouldRetry: true,
		},
		{
			desc:        "wrapped http2: client connection lost",
			inputErr:    fmt.Errorf("wrapped error: %w", &url.Error{Op: "blah", URL: "blah", Err: errors.New("http2: client connection lost")}),
			shouldRetry: true,
		},
		{
			desc:        "server closed idle connection",
			inputErr:    &url.Error{Op: "blah", URL: "blah", Err: errors.New("http: server closed idle connection")},
			shouldRetry: true,
		},
		{
			desc:        "wrapped server closed idle connection",
			inputErr:    fmt.Errorf("wrapped error: %w", &url.Error{Op: "blah", URL: "blah", Err: errors.New("http: server closed idle connection")}),
			shouldRetry: true,
		},
		{
			desc:        "net.OpError with server closed idle connection",
			inputErr:    &net.OpError{Op: "read", Net: "tcp", Err: errors.New("server closed idle connection")},
			shouldRetry: true,
		},
		{
			desc:        "wrapped net.OpError with server closed idle connection",
			inputErr:    fmt.Errorf("wrapped error: %w", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("server closed idle connection")}),
			shouldRetry: true,
		},
		{
			desc:        "errWriteStall",
			inputErr:    errWriteStall,
			shouldRetry: true,
		},
		{
			desc:        "wrapped errWriteStall",
			inputErr:    fmt.Errorf("wrapped write stall: %w", errWriteStall),
			shouldRetry: true,
		},
	} {
		t.Run(test.desc, func(s *testing.T) {
			got := ShouldRetry(test.inputErr)

			if got != test.shouldRetry {
				s.Errorf("got %v, want %v", got, test.shouldRetry)
			}
		})
	}
}

func TestInvokeWithCookie(t *testing.T) {
	expectedCookie := "C=a_test_cookie"
	oldCookieHeader := cookieHeader
	cookieHeader = func() string { return expectedCookie }
	defer func() {
		cookieHeader = oldCookieHeader
	}()

	ctx := context.Background()
	var gotCookie, gotDirectpathCookie string
	if err := run(ctx, func(ctx context.Context) error {
		headers := callctx.HeadersFromContext(ctx)
		gotCookie = headers["cookie"][0]
		gotDirectpathCookie = headers["x-directpath-tracing-cookie"][0]
		return nil
	}, nil, false); err != nil {
		t.Errorf("error during run; got %v, want nil", err)
	}

	if gotCookie != expectedCookie {
		t.Errorf("incorrect value for cookie header; got %v, want %v", gotCookie, expectedCookie)
	}

	if gotDirectpathCookie != expectedCookie {
		t.Errorf("incorrect value for x-directpath-tracing-cookie header; got %v, want %v", gotDirectpathCookie, expectedCookie)
	}
}

func TestInvokeWithRetryContext(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		desc          string
		operation     string
		bucket        string
		object        string
		retryAttempts int
		errorToReturn error
	}{
		{
			desc:          "RetryContext populated for object operations",
			operation:     "GetObject",
			bucket:        "test-bucket",
			object:        "test-object.txt",
			retryAttempts: 3,
			errorToReturn: &googleapi.Error{Code: 429},
		},
		{
			desc:          "RetryContext populated with empty object",
			operation:     "ListObjects",
			bucket:        "test-bucket",
			object:        "",
			retryAttempts: 2,
			errorToReturn: status.Error(codes.ResourceExhausted, "rate limit"),
		},
		{
			desc:          "RetryContext populated for write operations",
			operation:     "WriteObject",
			bucket:        "my-bucket",
			object:        "path/to/file.dat",
			retryAttempts: 4,
			errorToReturn: &googleapi.Error{Code: 503},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(s *testing.T) {
			var capturedContexts []*RetryContext
			var lastInvocationID string
			callCounter := 0

			// Custom shouldRetry function that captures RetryContext.
			customRetry := &retryConfig{
				policy: RetryAlways,
				shouldRetry: func(err error, retryCtx *RetryContext) bool {
					// Don't retry on success
					if err == nil {
						return false
					}

					// Capture the context for verification.
					capturedContexts = append(capturedContexts, &RetryContext{
						Attempt:      retryCtx.Attempt,
						InvocationID: retryCtx.InvocationID,
						Operation:    retryCtx.Operation,
						Bucket:       retryCtx.Bucket,
						Object:       retryCtx.Object,
					})

					// Remember the invocation ID from the first call.
					if len(capturedContexts) == 1 {
						lastInvocationID = retryCtx.InvocationID
					}

					// Retry if we haven't reached the retry limit.
					// retryCtx.Attempt is the current attempt that just failed.
					return retryCtx.Attempt <= test.retryAttempts
				},
				backoff: &gax.Backoff{Initial: time.Millisecond},
			}

			call := func(ctx context.Context) error {
				callCounter++
				// Fail for the first retryAttempts calls, succeed on call retryAttempts+1.
				if callCounter <= test.retryAttempts {
					return test.errorToReturn
				}
				return nil
			}

			// Run with the operation metadata.
			err := run(ctx, call, customRetry, true,
				withOperation(test.operation),
				withBucket(test.bucket),
				withObject(test.object))

			if err != nil {
				s.Fatalf("expected nil error after retries, got: %v", err)
			}

			// Verify we got the expected number of retry contexts.
			// shouldRetry is called once per failed attempt.
			expectedCalls := test.retryAttempts
			if len(capturedContexts) != expectedCalls {
				s.Errorf("expected %d retry contexts, got %d", expectedCalls, len(capturedContexts))
			}

			// Verify each captured context
			for i, retryCtx := range capturedContexts {
				expectedAttempt := i + 1

				// Check attempt number.
				if retryCtx.Attempt != expectedAttempt {
					s.Errorf("attempt %d: expected Attempt=%d, got %d", i, expectedAttempt, retryCtx.Attempt)
				}

				// Check invocation ID is consistent.
				if retryCtx.InvocationID == "" {
					s.Errorf("attempt %d: InvocationID should not be empty", i)
				}
				if retryCtx.InvocationID != lastInvocationID {
					s.Errorf("attempt %d: InvocationID changed, expected %s, got %s", i, lastInvocationID, retryCtx.InvocationID)
				}

				// Check operation name.
				if retryCtx.Operation != test.operation {
					s.Errorf("attempt %d: expected Operation=%q, got %q", i, test.operation, retryCtx.Operation)
				}

				// Check bucket name.
				if retryCtx.Bucket != test.bucket {
					s.Errorf("attempt %d: expected Bucket=%q, got %q", i, test.bucket, retryCtx.Bucket)
				}

				// Check object name.
				if retryCtx.Object != test.object {
					s.Errorf("attempt %d: expected Object=%q, got %q", i, test.object, retryCtx.Object)
				}
			}
		})
	}
}

// Test that RetryContext fields are empty when run is called without
// any runOption. This verifies that the default values for
// these fields are empty or zero.
func TestInvokeWithRetryContextWithoutRunOptions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var capturedContext *RetryContext

	customRetry := &retryConfig{
		shouldRetry: func(err error, retryCtx *RetryContext) bool {
			capturedContext = &RetryContext{
				Attempt:      retryCtx.Attempt,
				InvocationID: retryCtx.InvocationID,
				Operation:    retryCtx.Operation,
				Bucket:       retryCtx.Bucket,
				Object:       retryCtx.Object,
			}
			return false // Don't retry.
		},
		backoff: &gax.Backoff{Initial: time.Millisecond},
	}

	call := func(ctx context.Context) error {
		return &googleapi.Error{Code: 429}
	}

	// Run without any metadata options.
	_ = run(ctx, call, customRetry, true)

	// Verify context was captured.
	if capturedContext == nil {
		t.Fatal("RetryContext was not passed to shouldRetry function")
	}

	// Verify basic fields are set.
	if capturedContext.Attempt != 1 {
		t.Errorf("expected Attempt=1, got %d", capturedContext.Attempt)
	}

	if capturedContext.InvocationID == "" {
		t.Error("InvocationID should not be empty")
	}

	// Verify metadata fields are empty when not provided.
	if capturedContext.Operation != "" {
		t.Errorf("expected empty Operation, got %q", capturedContext.Operation)
	}

	if capturedContext.Bucket != "" {
		t.Errorf("expected empty Bucket, got %q", capturedContext.Bucket)
	}

	if capturedContext.Object != "" {
		t.Errorf("expected empty Object, got %q", capturedContext.Object)
	}
}

func TestIsError(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		httpErrorCode int
		grpcErrorCode codes.Code
		want          bool
	}{
		{
			name:          "matching HTTP error",
			err:           &googleapi.Error{Code: 404},
			httpErrorCode: 404,
			grpcErrorCode: codes.NotFound,
			want:          true,
		},
		{
			name:          "matching gRPC error",
			err:           status.Error(codes.NotFound, "not found"),
			httpErrorCode: 404,
			grpcErrorCode: codes.NotFound,
			want:          true,
		},
		{
			name:          "wrapped matching HTTP error",
			err:           fmt.Errorf("wrapped: %w", &googleapi.Error{Code: 404}),
			httpErrorCode: 404,
			grpcErrorCode: codes.NotFound,
			want:          true,
		},
		{
			name:          "wrapped matching gRPC error",
			err:           fmt.Errorf("wrapped: %w", status.Error(codes.NotFound, "not found")),
			httpErrorCode: 404,
			grpcErrorCode: codes.NotFound,
			want:          true,
		},
		{
			name:          "non-matching HTTP error",
			err:           &googleapi.Error{Code: 403},
			httpErrorCode: 404,
			grpcErrorCode: codes.NotFound,
			want:          false,
		},
		{
			name:          "non-matching gRPC error",
			err:           status.Error(codes.PermissionDenied, "permission denied"),
			httpErrorCode: 404,
			grpcErrorCode: codes.NotFound,
			want:          false,
		},
		{
			name:          "nil error",
			err:           nil,
			httpErrorCode: 404,
			grpcErrorCode: codes.NotFound,
			want:          false,
		},
		{
			name:          "unrelated error",
			err:           errors.New("some other error"),
			httpErrorCode: 404,
			grpcErrorCode: codes.NotFound,
			want:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isError(tt.err, tt.httpErrorCode, tt.grpcErrorCode); got != tt.want {
				t.Errorf("isError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInvoke_ErrWriteStall(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	attempts := 0
	call := func(ctx context.Context) error {
		attempts++
		if attempts <= 2 {
			return fmt.Errorf("write stall error: %w", errWriteStall)
		}
		return nil
	}

	retry := &retryConfig{
		backoff: &gax.Backoff{Initial: time.Millisecond},
	}
	err := run(ctx, call, retry, true)
	if err != nil {
		t.Fatalf("expected nil error after retry, got: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestInvoke_SessionRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		desc              string
		isIdempotent      bool
		retryPolicy       RetryPolicy
		setupBudget       func() *retryBudget
		callErrors        []error
		wantAttempts      int
		wantErr           bool
		expectedErrTarget error
	}{
		{
			desc:         "non-idempotent without retryBudget does not retry transient error",
			isIdempotent: false,
			setupBudget:  func() *retryBudget { return nil },
			callErrors: []error{
				status.Error(codes.Unavailable, "transient unavailable"),
			},
			wantAttempts:      1,
			wantErr:           true,
			expectedErrTarget: status.Error(codes.Unavailable, "transient unavailable"),
		},
		{
			desc:         "non-idempotent with retryBudget but no session does not retry transient error",
			isIdempotent: false,
			setupBudget: func() *retryBudget {
				return &retryBudget{
					hasSession: func() bool { return false },
				}
			},
			callErrors: []error{
				status.Error(codes.Unavailable, "transient unavailable"),
			},
			wantAttempts:      1,
			wantErr:           true,
			expectedErrTarget: status.Error(codes.Unavailable, "transient unavailable"),
		},
		{
			desc:         "non-idempotent with session retries transient error and succeeds",
			isIdempotent: false,
			setupBudget: func() *retryBudget {
				return &retryBudget{
					hasSession: func() bool { return true },
				}
			},
			callErrors: []error{
				status.Error(codes.Unavailable, "transient unavailable"),
				nil,
			},
			wantAttempts: 2,
			wantErr:      false,
		},
		{
			desc:         "non-idempotent with session retries errWriteStall and succeeds",
			isIdempotent: false,
			setupBudget: func() *retryBudget {
				return &retryBudget{
					hasSession: func() bool { return true },
				}
			},
			callErrors: []error{
				errWriteStall,
				nil,
			},
			wantAttempts: 2,
			wantErr:      false,
		},
		{
			desc:         "non-idempotent where session is established during first attempt retries",
			isIdempotent: false,
			setupBudget: func() *retryBudget {
				sessionActive := false
				return &retryBudget{
					hasSession: func() bool { return sessionActive },
					onProgress: func() { sessionActive = true },
				}
			},
			callErrors: []error{
				status.Error(codes.Unavailable, "transient unavailable"),
				nil,
			},
			wantAttempts: 2,
			wantErr:      false,
		},
		{
			desc:         "non-idempotent with session does not retry non-transient error",
			isIdempotent: false,
			setupBudget: func() *retryBudget {
				return &retryBudget{
					hasSession: func() bool { return true },
				}
			},
			callErrors: []error{
				&googleapi.Error{Code: 400},
			},
			wantAttempts: 1,
			wantErr:      true,
		},
		{
			desc:         "non-idempotent with RetryNever does not retry even with session",
			isIdempotent: false,
			retryPolicy:  RetryNever,
			setupBudget: func() *retryBudget {
				return &retryBudget{
					hasSession: func() bool { return true },
				}
			},
			callErrors: []error{
				status.Error(codes.Unavailable, "transient unavailable"),
			},
			wantAttempts: 1,
			wantErr:      true,
		},
		{
			desc:         "idempotent retries transient error even when session does not exist",
			isIdempotent: true,
			setupBudget: func() *retryBudget {
				return &retryBudget{
					hasSession: func() bool { return false },
				}
			},
			callErrors: []error{
				status.Error(codes.Unavailable, "transient unavailable"),
				nil,
			},
			wantAttempts: 2,
			wantErr:      false,
		},
		{
			desc:         "non-idempotent stops retrying when session is lost",
			isIdempotent: false,
			setupBudget: func() *retryBudget {
				sessionActive := true
				b := &retryBudget{}
				b.hasSession = func() bool { return sessionActive }
				return b
			},
			callErrors: []error{
				status.Error(codes.Unavailable, "transient 1"),
				status.Error(codes.Unavailable, "transient 2 (session lost)"),
			},
			wantAttempts: 2,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			attempts := 0
			var budget *retryBudget
			if tt.setupBudget != nil {
				budget = tt.setupBudget()
			}

			call := func(ctx context.Context) error {
				attempts++
				if attempts == 1 && strings.Contains(tt.desc, "session is established during first attempt") {
					budget.reportProgress()
				}
				if attempts == 2 && strings.Contains(tt.desc, "session is lost") {
					budget.hasSession = func() bool { return false }
				}
				if attempts <= len(tt.callErrors) {
					return tt.callErrors[attempts-1]
				}
				return nil
			}

			retry := &retryConfig{
				policy:  tt.retryPolicy,
				backoff: &gax.Backoff{Initial: time.Millisecond},
			}

			var opts []runOption
			if budget != nil {
				opts = append(opts, withRetryBudget(budget))
			}

			err := run(ctx, call, retry, tt.isIdempotent, opts...)
			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if attempts != tt.wantAttempts {
				t.Errorf("got %d attempts, want %d", attempts, tt.wantAttempts)
			}
			if tt.expectedErrTarget != nil && !errors.Is(err, tt.expectedErrTarget) {
				t.Errorf("got error %v, want target %v", err, tt.expectedErrTarget)
			}
		})
	}
}

func TestInvoke_PerChunkRetryBudget_AttemptsReset(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	maxAttempts := 3
	budget := &retryBudget{
		hasSession: func() bool { return true },
	}

	attempts := 0
	var recordedAttemptHeaders []string

	call := func(ctx context.Context) error {
		attempts++
		headers := callctx.HeadersFromContext(ctx)
		if clientHeader, ok := headers["x-goog-api-client"]; ok && len(clientHeader) > 0 {
			recordedAttemptHeaders = append(recordedAttemptHeaders, clientHeader[0])
		}

		switch attempts {
		case 1:
			// Chunk 1 attempt 1 fails
			return status.Error(codes.Unavailable, "chunk 1 fail 1")
		case 2:
			// Chunk 1 attempt 2 fails
			return status.Error(codes.Unavailable, "chunk 1 fail 2")
		case 3:
			// Chunk 1 succeeds, progress is strictly advanced!
			budget.recordProgress(16 * 1024 * 1024)
			// Chunk 2 then fails
			return status.Error(codes.Unavailable, "chunk 2 fail 1")
		case 4:
			// Chunk 2 attempt 2 fails (no new progress)
			return status.Error(codes.Unavailable, "chunk 2 fail 2")
		case 5:
			// Chunk 2 attempt 3 succeeds
			budget.recordProgress(32 * 1024 * 1024)
			return nil
		default:
			return nil
		}
	}

	retry := &retryConfig{
		maxAttempts: &maxAttempts,
		backoff:     &gax.Backoff{Initial: time.Millisecond},
	}

	err := run(ctx, call, retry, true, withRetryBudget(budget))
	if err != nil {
		t.Fatalf("expected success with per-chunk retry budget, got: %v", err)
	}

	if attempts != 5 {
		t.Fatalf("expected 5 total attempts across chunks, got %d", attempts)
	}

	// Verify attempt count in headers:
	// Attempt 1: gccl-attempt-count/1
	// Attempt 2: gccl-attempt-count/2
	// Attempt 3: gccl-attempt-count/3
	// Attempt 4 (Chunk 2 retry 1, after progress reset): gccl-attempt-count/2
	// Attempt 5 (Chunk 2 retry 2): gccl-attempt-count/3
	expectedAttemptCounts := []string{"1", "2", "3", "2", "3"}
	if len(recordedAttemptHeaders) != len(expectedAttemptCounts) {
		t.Fatalf("recorded %d headers, want %d", len(recordedAttemptHeaders), len(expectedAttemptCounts))
	}
	for i, wantCount := range expectedAttemptCounts {
		wantSub := fmt.Sprintf("gccl-attempt-count/%s", wantCount)
		if !strings.Contains(recordedAttemptHeaders[i], wantSub) {
			t.Errorf("call %d: header %q does not contain %q", i+1, recordedAttemptHeaders[i], wantSub)
		}
	}
}

func TestInvoke_PerChunkRetryBudget_NonAdvancingProgressDoesNotReset(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	maxAttempts := 3
	budget := &retryBudget{
		hasSession:    func() bool { return true },
		lastPersisted: 100,
	}

	attempts := 0
	call := func(ctx context.Context) error {
		attempts++
		// Re-reporting the same or lower offset does not strictly advance progress.
		budget.recordProgress(100)
		budget.recordProgress(50)
		return status.Error(codes.Unavailable, "unavailable")
	}

	retry := &retryConfig{
		maxAttempts: &maxAttempts,
		backoff:     &gax.Backoff{Initial: time.Millisecond},
	}

	err := run(ctx, call, retry, true, withRetryBudget(budget))
	if err == nil {
		t.Fatalf("expected error due to maxAttempts reached, got nil")
	}
	if attempts != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", attempts)
	}
	if !strings.Contains(err.Error(), "retry failed after 3 attempts") {
		t.Errorf("expected maxAttempts error, got: %v", err)
	}
}

func TestInvoke_PerChunkRetryBudget_BackoffReset(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	budget := &retryBudget{
		hasSession: func() bool { return true },
	}

	// We use initial backoff of 10ms with Multiplier=100.
	// Failure 1: cur=10ms, next cur becomes 1000ms (1s).
	// If progress is NOT reset, failure 2 pause would be bounded by 1000ms.
	// When progress IS reset, cur resets to Initial (10ms), so pause remains <= 10ms!
	attempts := 0

	call := func(ctx context.Context) error {
		attempts++
		if attempts == 1 {
			// First failure, no progress
			return status.Error(codes.Unavailable, "transient 1")
		}
		if attempts == 2 {
			// Progress made! Strictly advanced
			budget.recordProgress(1024)
			// Second failure
			return status.Error(codes.Unavailable, "transient 2")
		}
		return nil
	}

	retry := &retryConfig{
		backoff: &gax.Backoff{
			Initial:    10 * time.Millisecond,
			Multiplier: 100,
			Max:        30 * time.Second,
		},
	}

	start := time.Now()
	err := run(ctx, call, retry, true, withRetryBudget(budget))
	totalElapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}

	// Because backoff was reset to 10ms for attempt 2 failure, total elapsed time
	// should be well under 500ms (both pauses are <= 10ms each plus jitter).
	// If it had compounded with Multiplier 100, attempt 2's pause would have had cur=1000ms (up to 1s sleep).
	if totalElapsed > 500*time.Millisecond {
		t.Errorf("total elapsed time %v exceeded 500ms; backoff was likely not reset to initial", totalElapsed)
	}
}

func TestRetryBudget_Methods(t *testing.T) {
	t.Parallel()

	t.Run("recordProgress strictly advances", func(t *testing.T) {
		b := &retryBudget{}

		// 0 is not > 0
		if b.recordProgress(0) {
			t.Errorf("recordProgress(0) on initial 0 should return false")
		}
		if b.checkAndResetProgress() {
			t.Errorf("expected no progress made")
		}

		// 100 > 0
		if !b.recordProgress(100) {
			t.Errorf("recordProgress(100) should return true")
		}
		if b.lastPersistedOffset() != 100 {
			t.Errorf("expected lastPersistedOffset 100, got %d", b.lastPersistedOffset())
		}
		if !b.checkAndResetProgress() {
			t.Errorf("expected progressMade to be true")
		}
		// checkAndResetProgress should have reset the flag
		if b.checkAndResetProgress() {
			t.Errorf("expected progressMade to be reset to false")
		}

		// Same offset 100 should not advance
		if b.recordProgress(100) {
			t.Errorf("recordProgress(100) again should return false")
		}

		// Lower offset 50 should not advance
		if b.recordProgress(50) {
			t.Errorf("recordProgress(50) should return false")
		}

		// Higher offset 200 should advance
		if !b.recordProgress(200) {
			t.Errorf("recordProgress(200) should return true")
		}
	})

	t.Run("reportProgress marks progress", func(t *testing.T) {
		b := &retryBudget{}
		b.reportProgress()
		if !b.checkAndResetProgress() {
			t.Errorf("expected checkAndResetProgress to return true after reportProgress")
		}
	})

	t.Run("onProgress callback invoked on advancement", func(t *testing.T) {
		callbacks := 0
		b := &retryBudget{
			onProgress: func() { callbacks++ },
		}
		b.recordProgress(10) // callback 1
		b.recordProgress(10) // no advance, no callback
		b.recordProgress(20) // callback 2
		b.reportProgress()   // callback 3

		if callbacks != 3 {
			t.Errorf("expected 3 callbacks, got %d", callbacks)
		}
	})

	t.Run("hasActiveSession nil safety", func(t *testing.T) {
		var nilBudget *retryBudget
		if nilBudget.hasActiveSession() {
			t.Errorf("nil budget should return false for hasActiveSession")
		}

		emptyBudget := &retryBudget{}
		if emptyBudget.hasActiveSession() {
			t.Errorf("empty budget should return false for hasActiveSession")
		}

		activeBudget := &retryBudget{
			hasSession: func() bool { return true },
		}
		if !activeBudget.hasActiveSession() {
			t.Errorf("active budget should return true for hasActiveSession")
		}
	})

	t.Run("concurrent progress reporting", func(t *testing.T) {
		b := &retryBudget{}
		var wg sync.WaitGroup
		for i := int64(1); i <= 100; i++ {
			wg.Add(1)
			go func(offset int64) {
				defer wg.Done()
				b.recordProgress(offset)
			}(i)
		}
		wg.Wait()
		if b.lastPersistedOffset() != 100 {
			t.Errorf("expected lastPersistedOffset 100, got %d", b.lastPersistedOffset())
		}
	})
}
