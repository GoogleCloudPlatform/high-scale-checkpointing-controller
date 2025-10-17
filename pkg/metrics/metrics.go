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

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/component-base/metrics"
	"k8s.io/klog/v2"
)

const (
	metricsPath               = "/metrics"
	nodeOperationsSecondsName = "node_operations_seconds"
	timeoutDuration           = 120 * time.Second
	timeoutValue              = 150.0
)

var (
	// This metric is exposed only from the controller driver component when GKE_PDCSI_VERSION env variable is set.
	gkeComponentVersion = metrics.NewGaugeVec(&metrics.GaugeOpts{
		Name: "component_version",
		Help: "Metric to expose the version of the PDCSI GKE component.",
	}, []string{"component_version"})

	// Node metrics
	nodeOperationsSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    nodeOperationsSecondsName,
			Help:    "",
			Buckets: []float64{0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 40.0, 45.0, 55.0, 75.0, 120.0},
		},
		[]string{"method_name", "framework", "grpc_status_code"},
	)
)

// MetricsManager defines the interface for node metrics management.
type MetricsManager interface {
	InitializeHTTPHandler(metricsEndpoint string)
	EmitNodeOpsSeconds(methodName, framework, grpcStatusCode string, elapsed float64)
	RecordNodeOperationWithTimeout(ctx context.Context, methodName string) chan error
}

// nodeMetricsManager implements MetricsManager.
type nodeMetricsManager struct{}

// Ensure nodeMetricsManager implements MetricsManager.
var _ MetricsManager = &nodeMetricsManager{}

func NewMetricsManager() MetricsManager {
	return &nodeMetricsManager{}
}

func (mm *nodeMetricsManager) InitializeHTTPHandler(metricsEndpoint string) {
	// Register the metric with the default registry.
	prometheus.MustRegister(nodeOperationsSeconds)

	// Expose the registered metrics via an HTTP endpoint.
	go func() {
		klog.Infof("metric server listening at %q", metricsEndpoint)
		http.Handle(metricsPath, promhttp.Handler())
		if err := http.ListenAndServe(metricsEndpoint, nil); err != nil {
			klog.Errorf("Failed to start node metric server at specified address (%q) and path (%q): %v", metricsEndpoint, metricsPath, err)
		}
	}()
}

func (mm *nodeMetricsManager) EmitNodeOpsSeconds(methodName, framework, grpcStatusCode string, elapsed float64) {
	nodeOperationsSeconds.With(prometheus.Labels{"method_name": methodName, "framework": framework, "grpc_status_code": grpcStatusCode}).Observe(elapsed)
}

func (mm *nodeMetricsManager) RecordNodeOperationWithTimeout(ctx context.Context, methodName string) chan error {
	//TODO: add appropriate framework tag
	opDoneChan := make(chan error, 1)

	startTime := time.Now()

	go func() {
		select {
		case opErr := <-opDoneChan:
			elapsed := time.Since(startTime).Seconds()
			statusCode := codes.OK
			if opErr != nil {
				if st, ok := status.FromError(opErr); ok {
					statusCode = st.Code()
				} else if errors.Is(opErr, context.Canceled) {
					statusCode = codes.Canceled
				} else if errors.Is(opErr, context.DeadlineExceeded) {
					statusCode = codes.DeadlineExceeded
				} else {
					statusCode = codes.Internal
				}
			}
			mm.EmitNodeOpsSeconds(methodName, "", statusCode.String(), elapsed)

		case <-time.After(timeoutDuration):
			klog.Warningf("%s exceeded timeout of %v, emitting %f and returning DeadlineExceeded", methodName, timeoutDuration, timeoutValue)
			mm.EmitNodeOpsSeconds(methodName, "", codes.DeadlineExceeded.String(), timeoutValue)

		case <-ctx.Done():
			elapsed := time.Since(startTime).Seconds()
			mm.EmitNodeOpsSeconds(methodName, "", codes.Canceled.String(), elapsed)
		}
	}()

	return opDoneChan
}

// Controller Metrics Manager

type ControllerMetricsManager struct {
	registry metrics.KubeRegistry
}

func NewControllerMetricsManager() ControllerMetricsManager {
	return ControllerMetricsManager{registry: metrics.NewKubeRegistry()}
}

func (mm *ControllerMetricsManager) InitializeHTTPHandler(metricsEndpoint string) {
	mux := http.NewServeMux()
	mux.Handle(metricsPath, metrics.HandlerFor(
		mm.registry,
		metrics.HandlerOpts{
			ErrorHandling: metrics.ContinueOnError}))
	go func() {
		klog.Infof("Metric server listening at %q", metricsEndpoint)
		if err := http.ListenAndServe(metricsEndpoint, mux); err != nil {
			klog.Fatalf("Failed to start metric server at specified address (%q) and path (%q): %v", metricsEndpoint, metricsPath, err.Error())
		}
	}()
}

func (mm *ControllerMetricsManager) recordComponentVersionMetric() error {
	v := util.GetEnvVar(util.EnvGKEComponentVersion)
	if v == "" {
		klog.V(2).Info("Skip emitting component version metric")
		// return fmt.Errorf("Failed to register GKE component version metric, env variable %v not defined", envGKEHighScaleCheckpointingVersion)
	}

	gkeComponentVersion.WithLabelValues(v).Set(1.0)
	klog.Infof("Recorded GKE component version : %v", v)
	return nil
}

func (mm *ControllerMetricsManager) EmitGKEComponentVersion() error {
	mm.registry.MustRegister(gkeComponentVersion)
	if err := mm.recordComponentVersionMetric(); err != nil {
		return err
	}

	return nil
}
