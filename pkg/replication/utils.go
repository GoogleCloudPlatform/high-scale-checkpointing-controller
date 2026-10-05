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
	"context"

	"google.golang.org/grpc"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	utilnet "k8s.io/apimachinery/pkg/util/net"

	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/metrics"
)

func cappedLatencyInterceptor(mm metrics.MetricsManager) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// pass through if no metrics manager is configured.
		if mm == nil {
			return handler(ctx, req)
		}
		opDoneChan := mm.RecordNodeOperationWithTimeout(ctx, info.FullMethod)
		resp, err := handler(ctx, req)

		// Send returned error to the channel.
		// This is non-blocking because opDoneChan is buffered.
		opDoneChan <- err

		return resp, err
	}
}

func isTransientKubeError(err error) bool {
	if err == nil {
		return false
	}
	// Transport / connection errors (same checks client-go uses for GET retries, plus timeout/refused):
	if utilnet.IsHTTP2ConnectionLost(err) || // matches "http2: client connection lost"
		utilnet.IsProbableEOF(err) || // EOF, ErrUnexpectedEOF, GOAWAY, broken connection, closed network conn
		utilnet.IsConnectionReset(err) || // ECONNRESET ("connection reset by peer")
		utilnet.IsConnectionRefused(err) || // ECONNREFUSED
		utilnet.IsTimeout(err) { // net.Error Timeout()
		return true
	}
	// Transient API server status errors (500, 503, 504, 429):
	if k8serrors.IsInternalError(err) ||
		k8serrors.IsServiceUnavailable(err) ||
		k8serrors.IsServerTimeout(err) ||
		k8serrors.IsTimeout(err) ||
		k8serrors.IsTooManyRequests(err) {
		return true
	}
	return false
}
