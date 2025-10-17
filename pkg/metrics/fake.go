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

package metrics

import "context"

// FakeMetricsManager is a no-op implementation of MetricsManager for testing.
// Ensure FakeMetricsManager implements MetricsManager.
var _ MetricsManager = &FakeMetricsManager{}

type FakeMetricsManager struct{}

// NewFakeMetricsManager returns a new FakeMetricsManager.
func NewFakeMetricsManager() *FakeMetricsManager {
	return &FakeMetricsManager{}
}

// InitializeHTTPHandler is a no-op for FakeMetricsManager.
func (fmm *FakeMetricsManager) InitializeHTTPHandler(metricsEndpoint string) {
	// No-op
}

// EmitNodeOpsSeconds is a no-op for FakeMetricsManager.
func (fmm *FakeMetricsManager) EmitNodeOpsSeconds(methodName, framework, grpcStatusCode string, elapsed float64) {
	// No-op
}

// RecordNodeOperationWithTimeout is a no-op for FakeMetricsManager.
func (fmm *FakeMetricsManager) RecordNodeOperationWithTimeout(ctx context.Context, methodName string) chan error {
	return make(chan error, 1) // Returns a buffered channel, similar to the real one, but nothing will process it.
}
