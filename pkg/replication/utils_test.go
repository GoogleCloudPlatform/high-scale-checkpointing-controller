// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package replication

import (
	"errors"
	"io"
	"net/url"
	"syscall"
	"testing"

	"gotest.tools/v3/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIsTransientKubeError(t *testing.T) {
	gr := schema.GroupResource{Resource: "configmaps"}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{
			name: "http2 connection lost wrapped in url.Error",
			err:  &url.Error{Op: "Put", URL: "https://apiserver/api/v1/...", Err: errors.New("http2: client connection lost")},
			want: true,
		},
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF, want: true},
		{name: "connection reset", err: syscall.ECONNRESET, want: true},
		{name: "connection refused", err: syscall.ECONNREFUSED, want: true},
		{name: "503 service unavailable", err: apierrors.NewServiceUnavailable("apiserver overloaded"), want: true},
		{name: "500 internal error", err: apierrors.NewInternalError(errors.New("etcd timeout")), want: true},
		{name: "429 too many requests", err: apierrors.NewTooManyRequestsError("rate limited"), want: true},
		{name: "409 conflict is handled separately", err: apierrors.NewConflict(gr, "job", errors.New("conflict")), want: false},
		{name: "404 not found", err: apierrors.NewNotFound(gr, "job"), want: false},
		{name: "400 bad request", err: apierrors.NewBadRequest("invalid"), want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, isTransientKubeError(tc.err), tc.want)
		})
	}
}
