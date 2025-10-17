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

// These test coordinator and gcs bucket; peer replication is tested in e2e.

import (
	"context"
	"os"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	testutil "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/hack"
	proto "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/lib/grpc/replication/proto"
)

const (
	replicatorNamespace = "replication"
)

var (
	kubeClient *kubernetes.Clientset
	server     ReplicationServer
	rBaseDir   string
	pBaseDir   string
)

func TestMain(m *testing.M) {
	testutil.SetupEnviron(context.TODO())

	utilruntime.Must(scheme.AddToScheme(scheme.Scheme))

	m.Run()
}

func mustSetupCluster(t *testing.T) (context.Context, func(ctx context.Context)) {
	t.Helper()
	ctx, globalCancel := context.WithCancel(context.TODO())

	testEnv := &envtest.Environment{
		UseExistingCluster: ptr.To(false),
		CRDDirectoryPaths:  []string{"../../test/crds"},
	}
	var err error
	testCfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("cannot start testenv: %v", err)
	}

	kubeClient, err = kubernetes.NewForConfig(testCfg)
	if err != nil {
		t.Fatalf("cannot get kubeclient: %v", err)
	}

	if ns, err := kubeClient.CoreV1().Namespaces().Get(ctx, replicatorNamespace, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		ns.SetName(replicatorNamespace)
		if _, err := kubeClient.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil {
			t.Fatalf("can't create namespace %s: %v", replicatorNamespace, err)
		}
	}

	rBaseDir, err = os.MkdirTemp("/tmp", "replication_test-")
	if err != nil {
		t.Fatalf("can't create tmpdir: %v", err)
	}

	pBaseDir, err = os.MkdirTemp("/tmp", "replication_test-")
	if err != nil {
		t.Fatalf("can't create tmpdir: %v", err)
	}

	server, err = NewServer(kubeClient, ServerOptions{
		Namespace:      replicatorNamespace,
		RemoteBase:     rBaseDir,
		PersistentBase: pBaseDir,
	})
	if err != nil {
		t.Fatalf("can't create server: %v", err)
	}

	return ctx, func(_ context.Context) {
		server.Close()
		globalCancel()
		if err := os.RemoveAll(rBaseDir); err != nil {
			t.Errorf("tmpdir remove error: %v", err)
		}
		if err := testEnv.Stop(); err != nil {
			t.Errorf("testenv shutdown: %v", err)
		}
	}
}

func TestCoordinatorSetRace(t *testing.T) {
	ctx, cleanup := mustSetupCluster(t)
	defer cleanup(ctx)

	_, err := server.RegisterCoordinator(ctx, &proto.RegisterCoordinatorRequest{JobName: "job", Ip: "10.0.0.1"})
	assert.NilError(t, err)
	_, err = server.UnregisterCoordinator(ctx, &proto.UnregisterCoordinatorRequest{JobName: "job", Ip: "10.0.0.1"})
	assert.NilError(t, err)

	var reg = make(chan int)
	go func() {
		_, err := server.RegisterCoordinator(ctx, &proto.RegisterCoordinatorRequest{JobName: "job", Ip: "10.0.0.3"})
		assert.Check(t, err == nil)
		reg <- 0
	}()
	_, err = server.RegisterCoordinator(ctx, &proto.RegisterCoordinatorRequest{JobName: "job", Ip: "10.0.0.2"})
	assert.NilError(t, err)
	rsp, err := server.GetCoordinator(ctx, &proto.GetCoordinatorRequest{JobName: "job"})
	assert.NilError(t, err)
	assert.Assert(t, rsp.Ip == "10.0.0.2" || rsp.Ip == "10.0.0.3")
	<-reg
}

func TestCoordinatorSetAndUnset(t *testing.T) {
	ctx, cleanup := mustSetupCluster(t)
	defer cleanup(ctx)

	var delayedGet = make(chan string)

	go func() {
		deadline, cancel := context.WithDeadline(ctx, time.Now().Add(time.Minute))
		rsp, err := server.GetCoordinator(deadline, &proto.GetCoordinatorRequest{JobName: "job"})
		assert.NilError(t, err)
		cancel()
		delayedGet <- rsp.Ip
	}()

	_, err := server.RegisterCoordinator(ctx, &proto.RegisterCoordinatorRequest{JobName: "job", Ip: "10.0.0.1"})
	assert.NilError(t, err)
	deadline, cancel := context.WithDeadline(ctx, time.Now().Add(time.Second))
	_, err = server.GetCoordinator(deadline, &proto.GetCoordinatorRequest{JobName: "foo"})
	assert.ErrorContains(t, err, "deadline")
	cancel()
	rsp, err := server.GetCoordinator(ctx, &proto.GetCoordinatorRequest{JobName: "job"})
	assert.NilError(t, err)
	assert.Equal(t, rsp.Ip, "10.0.0.1")
	assert.Equal(t, <-delayedGet, "10.0.0.1")

	_, err = server.UnregisterCoordinator(ctx, &proto.UnregisterCoordinatorRequest{JobName: "foo", Ip: "10.0.0.1"})
	assert.NilError(t, err)
	rsp, err = server.GetCoordinator(ctx, &proto.GetCoordinatorRequest{JobName: "job"})
	assert.NilError(t, err)
	assert.Equal(t, rsp.Ip, "10.0.0.1")

	_, err = server.UnregisterCoordinator(ctx, &proto.UnregisterCoordinatorRequest{JobName: "job", Ip: "10.0.0.1"})
	assert.NilError(t, err)
	deadline, cancel = context.WithDeadline(ctx, time.Now().Add(time.Second))
	_, err = server.GetCoordinator(deadline, &proto.GetCoordinatorRequest{JobName: "job"})
	assert.ErrorContains(t, err, "deadline")
	cancel()
}

func TestCoordinatorUnsetRace(t *testing.T) {
	ctx, cleanup := mustSetupCluster(t)
	defer cleanup(ctx)

	_, err := server.RegisterCoordinator(ctx, &proto.RegisterCoordinatorRequest{JobName: "job", Ip: "10.0.0.1"})
	assert.NilError(t, err)
	rsp, err := server.GetCoordinator(ctx, &proto.GetCoordinatorRequest{JobName: "job"})
	assert.NilError(t, err)
	assert.Equal(t, rsp.Ip, "10.0.0.1")

	_, err = server.RegisterCoordinator(ctx, &proto.RegisterCoordinatorRequest{JobName: "job", Ip: "10.0.0.2"})
	assert.NilError(t, err)
	rsp, err = server.GetCoordinator(ctx, &proto.GetCoordinatorRequest{JobName: "job"})
	assert.NilError(t, err)
	assert.Equal(t, rsp.Ip, "10.0.0.2")

	_, err = server.UnregisterCoordinator(ctx, &proto.UnregisterCoordinatorRequest{JobName: "job", Ip: "10.0.0.1"})
	assert.NilError(t, err)
	rsp, err = server.GetCoordinator(ctx, &proto.GetCoordinatorRequest{JobName: "job"})
	assert.NilError(t, err)
	assert.Equal(t, rsp.Ip, "10.0.0.2")
}
