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
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	informerv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	lister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	proto "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/lib/grpc/replication/proto"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/csi"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/metrics"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/util"
)

const (
	resyncPeriod          = 5 * time.Minute
	sentinalPrefix        = ".peer-mount-"
	coordinatorAddressKey = "ip"
	mountCmd              = "/bin/mount"
	umountCmd             = "/bin/umount"
)

type ServerOptions struct {
	Namespace      string
	CpcName        string
	PeerBase       string
	RemoteBase     string
	PersistentBase string
	NfsExport      string
	MetricsManager metrics.MetricsManager
}

type replicationServer struct {
	kubeClient        *kubernetes.Clientset
	watcherFlag       chan struct{}
	configMapLister   lister.ConfigMapLister
	configMapInformer informerv1.ConfigMapInformer
	informerCloser    chan struct{}
	opts              ServerOptions

	mm metrics.MetricsManager
}

type ReplicationServer interface {
	proto.ReplicationServiceServer
	Start(port int) error
	Close()
}

var _ ReplicationServer = &replicationServer{}

func NewServer(kubeClient *kubernetes.Clientset, opts ServerOptions) (ReplicationServer, error) {
	factory := informers.NewSharedInformerFactoryWithOptions(kubeClient, resyncPeriod, informers.WithNamespace(opts.Namespace))
	informer := factory.Core().V1().ConfigMaps()

	server := &replicationServer{
		kubeClient:        kubeClient,
		watcherFlag:       make(chan struct{}, 1),
		configMapInformer: informer,
		configMapLister:   informer.Lister(),
		informerCloser:    make(chan struct{}),
		opts:              opts,
		mm:                opts.MetricsManager,
	}

	_, err := informer.Informer().AddEventHandler(&cache.ResourceEventHandlerFuncs{
		AddFunc:    func(_ interface{}) { server.raiseWatcherFlag() },
		UpdateFunc: func(_, _ interface{}) { server.raiseWatcherFlag() },
	})
	if err != nil {
		return nil, err
	}

	// Start informer.
	go func() {
		stopCh := make(chan struct{})
		defer close(stopCh)
		factory.Start(stopCh)
		klog.Info("Starting configmap cache sync")
		if !cache.WaitForCacheSync(stopCh, informer.Informer().HasSynced) {
			klog.Fatal("Cannot sync caches")
		}
		klog.Info("Cache synced")

		// Stop on SIGINT or closer
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		select {
		case <-c:
			klog.Info("closing replication informer due to signal")
		case <-server.informerCloser:
			klog.Info("closing replication informer from request")
		}
	}()

	return server, nil
}

// Start fires up a gRPC server, listening on port. It will not return unless there is an error.
func (r *replicationServer) Start(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(csi.LogGRPC, cappedLatencyInterceptor(r.mm)))
	proto.RegisterReplicationServiceServer(s, r)
	klog.Infof("replication api server listening at %v", lis.Addr())
	return s.Serve(lis)
}

// Close closes any proceses like the informer of a server.
func (r *replicationServer) Close() {
	klog.Infof("replication api server closing...")
	close(r.informerCloser)
}

// GetCoordinator implements proto.ReplicationServiceServer.
func (r *replicationServer) GetCoordinator(ctx context.Context, req *proto.GetCoordinatorRequest) (*proto.GetCoordinatorResponse, error) {
	klog.Infof("GetCoordinator called for job %q", req.JobName)
	if req.JobName == "" {
		return nil, status.Error(codes.InvalidArgument, "missing jobname")
	}
	ip, err := r.fetchJobCoordinator(ctx, req.JobName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &proto.GetCoordinatorResponse{Ip: ip}, nil
}

// UnregisterCoordinator implements replication.ReplicationServiceServer.
func (r *replicationServer) UnregisterCoordinator(ctx context.Context, req *proto.UnregisterCoordinatorRequest) (*proto.UnregisterCoordinatorResponse, error) {
	if req.JobName == "" || req.Ip == "" {
		return nil, status.Errorf(codes.InvalidArgument, "bad jobname or IP in %v", req)
	}
	configMap, err := r.queryCoordinatorConfigMap(ctx, req.JobName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "fetching configmap: %v", err)
	}
	if configMap == nil || configMap.Data == nil {
		// Nothing to do.
		return &proto.UnregisterCoordinatorResponse{}, nil
	}
	oldIp, found := configMap.Data[coordinatorAddressKey]
	if !found || oldIp != req.Ip {
		return &proto.UnregisterCoordinatorResponse{}, nil
	}
	configMap.Data[coordinatorAddressKey] = ""

	if _, err := r.kubeClient.CoreV1().ConfigMaps(r.opts.Namespace).Update(ctx, configMap, metav1.UpdateOptions{}); err != nil {
		if k8serrors.IsConflict(err) {
			// Conflict, do nothing
			return &proto.UnregisterCoordinatorResponse{}, nil
		}
		return nil, status.Errorf(codes.Unavailable, "%v", err)
	}

	return &proto.UnregisterCoordinatorResponse{}, nil
}

// MountGCSBucket implements proto.ReplicationServiceServer.
func (r *replicationServer) MountGCSBucket(ctx context.Context, req *proto.MountGCSBucketRequest) (*proto.MountGCSBucketResponse, error) {
	if req.LocalMountpoint == "" || strings.Contains(req.LocalMountpoint, "/") {
		return nil, status.Errorf(codes.InvalidArgument, "bad mountpoint %s", req.LocalMountpoint)
	}

	persistentTarget := filepath.Join(r.opts.PersistentBase, req.LocalMountpoint)
	mounter := mount.New("")
	retryInterval := 1 * time.Second

	// unmount existing remote bindmount if already exists
	if checkSentinal(r.opts.RemoteBase) {
		err := retryWithContext(ctx, func() error {
			return mounter.Unmount(r.opts.RemoteBase)
		}, "unmount pre-existing gcsFuse bind mountpoint", retryInterval)

		if err != nil {
			return nil, err
		}
		// unmount done, try to remove sentinal file
		sentinalErr := removeSentinal(r.opts.RemoteBase)
		if sentinalErr != nil {
			klog.Warningf("successfully unmounted pre-existing bindmount at %s, error trying to remove sentinal: %v", r.opts.RemoteBase, sentinalErr)
		}
	}

	if err := retryWithContext(ctx, func() error {
		return os.MkdirAll(persistentTarget, 0777)
	}, fmt.Sprintf("create directory %s", persistentTarget), retryInterval); err != nil {
		return nil, err
	}

	if err := retryWithContext(ctx, func() error {
		return os.MkdirAll(r.opts.RemoteBase, 0777)
	}, fmt.Sprintf("create directory %s", r.opts.RemoteBase), retryInterval); err != nil {
		return nil, err
	}

	mount_options := []string{"bind"}
	if err := retryWithContext(ctx, func() error {
		return mounter.Mount(persistentTarget, r.opts.RemoteBase, "", mount_options)
	}, fmt.Sprintf("bind mount %s to %s", persistentTarget, r.opts.RemoteBase), retryInterval); err != nil {
		return nil, err
	}

	if err := retryWithContext(ctx, func() error {
		return makeSentinal(r.opts.RemoteBase)
	}, fmt.Sprintf("make sentinal for %s", r.opts.RemoteBase), retryInterval); err != nil {
		return nil, err
	}

	return &proto.MountGCSBucketResponse{}, nil
}

// RegisterCoordinator implements replication.ReplicationServiceServer.
func (r *replicationServer) RegisterCoordinator(ctx context.Context, req *proto.RegisterCoordinatorRequest) (*proto.RegisterCoordinatorResponse, error) {
	if req.JobName == "" || req.Ip == "" {
		return nil, status.Errorf(codes.InvalidArgument, "bad jobname or IP in %v", req)
	}
	configMap, err := r.queryCoordinatorConfigMap(ctx, req.JobName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "err fetching configmap %q from cache: %v", req.JobName, err)
	}
	needCreate := false
	if configMap == nil {
		// coordinator info not found in cache, hit the API server
		configMap, err = r.getCoordinatorConfigMapFromKubeAPI(ctx, req.JobName)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "error fetching configmap %q from API server: %v", req.JobName, err)
		}
		if configMap == nil {
			needCreate = true
			configMap = &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      req.JobName,
					Namespace: r.opts.Namespace,
					Annotations: map[string]string{
						util.CpcAnnotation: r.opts.CpcName,
					},
				},
			}
		}
	}
	if configMap.Data == nil {
		configMap.Data = map[string]string{}
	}
	configMap.Data[coordinatorAddressKey] = req.Ip

	if needCreate {
		klog.Infof("coordinator info for job %q not found, creating %v", req.JobName, configMap)
		if _, err := r.kubeClient.CoreV1().ConfigMaps(r.opts.Namespace).Create(ctx, configMap, metav1.CreateOptions{}); err != nil {
			return nil, status.Errorf(codes.Internal, "creating configmap: %v", err)
		}
	} else if err := wait.PollImmediateWithContext(ctx, time.Second, 30*time.Second, func(ctx context.Context) (bool, error) {
		var err error
		configMap, err = r.kubeClient.CoreV1().ConfigMaps(r.opts.Namespace).Update(ctx, configMap, metav1.UpdateOptions{})
		if k8serrors.IsConflict(err) {
			klog.Infof("coordinator info update conflicted, retrying...")
			configMap, err = r.getCoordinatorConfigMapFromKubeAPI(ctx, req.JobName)
			if err != nil {
				klog.Infof("(retrying) error fetching up-to-date configmap %q from API server: %v", req.JobName, err)
				return false, nil // retry
			}
			if configMap == nil {
				// this shouldn't really happen. weird stuff is going on if this happens.
				return false, status.Errorf(codes.Unavailable, "configMap %q not found after update conflict, please retry: %v", req.JobName, err)
			}
			if configMap.Data == nil {
				configMap.Data = map[string]string{}
			}
			configMap.Data[coordinatorAddressKey] = req.Ip
			return false, nil // retry
		} else if err != nil {
			return false, status.Errorf(codes.Internal, "Unknown configmap update error: %v", err)
		}
		return true, nil
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "updating configmap %v: %v", configMap, err)
	}

	return &proto.RegisterCoordinatorResponse{}, nil
}

// getCoordinatorConfigMapFromKubeAPI looks for a job configmap from the API server and returns one, or nil if not found.
//
// An error is returned only on a non-retriable error (eg, timeout).
// The key difference between this function and queryCoordinatorConfigMap is this function hits the Kube
// API server directly and should only be used if the most up-to-date state is required.
func (r *replicationServer) getCoordinatorConfigMapFromKubeAPI(ctx context.Context, job string) (*corev1.ConfigMap, error) {
	configMap, err := r.kubeClient.CoreV1().ConfigMaps(r.opts.Namespace).Get(ctx, job, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return configMap, nil
}

// SetReplicationPeer implements replication.ReplicationServiceServer.
func (r *replicationServer) SetReplicationPeer(ctx context.Context, req *proto.SetReplicationPeerRequest) (*proto.SetReplicationPeerResponse, error) {
	klog.Infof("SetReplicationPeer called with req %v", req)
	if req.LocalMountpoint == "" || strings.Contains(req.LocalMountpoint, "/") || req.TargetIp == "" {
		return nil, status.Errorf(codes.InvalidArgument, "bad mountpoint or IP in %v", req)
	}
	target := filepath.Join(r.opts.PeerBase, req.LocalMountpoint)
	if err := os.MkdirAll(target, 0777); err != nil {
		return nil, status.Errorf(codes.Internal, "making target %s: %v", target, err.Error())
	}
	if err := makeSentinal(target); err != nil {
		return nil, status.Errorf(codes.Internal, "making sentinal for %s: %v", target, err.Error())
	}

	opts := []string{
		"nconnect=16",
	}
	optString := strings.Join(opts, ",")

	// todo: deadline on the mount. ctx will be canceled but we'd like to cancel
	// this command (although it will timeout by itself?)
	if _, err := util.RunCommand(mountCmd, "-t", "nfs", fmt.Sprintf("%s:%s", req.TargetIp, r.opts.NfsExport), "-o", optString, target); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &proto.SetReplicationPeerResponse{}, nil
}

// UnmountPeer implements replication.ReplicationServiceServer.
func (r *replicationServer) UnmountPeer(ctx context.Context, req *proto.UnmountPeerRequest) (*proto.UnmountPeerResponse, error) {
	klog.Infof("UnmountPeer called with req %v", req)
	if req.LocalMountpoint == "" || strings.Contains(req.LocalMountpoint, "/") {
		return nil, status.Errorf(codes.InvalidArgument, "bad mountpoint in %v", req)
	}
	if err := r.unmountPeerInternal(ctx, req.LocalMountpoint); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &proto.UnmountPeerResponse{}, nil
}

// UnmountAllPeers implements replication.ReplicationServiceServer.
func (r *replicationServer) UnmountAllPeers(ctx context.Context, req *proto.UnmountAllPeersRequest) (*proto.UnmountAllPeersResponse, error) {
	klog.Info("UnmountAllPeers called")
	mountPoints, err := getPeerMounts(r.opts.PeerBase)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot list peers: %v", err)
	}
	errs := []error{}
	for _, target := range mountPoints {
		if err := r.unmountPeerInternal(ctx, target); err != nil {
			errs = append(errs, fmt.Errorf("while unmounting %s: %w", target, err))
		}
	}
	if len(errs) > 0 {
		return nil, status.Errorf(codes.Internal, "unmount errors: %v", errs)
	}
	return &proto.UnmountAllPeersResponse{}, nil
}

func (r *replicationServer) unmountPeerInternal(ctx context.Context, peerDir string) error {
	target := filepath.Join(r.opts.PeerBase, peerDir)
	klog.Infof("trying unmount of %s", target)
	if _, err := os.Stat(target); errors.Is(err, os.ErrNotExist) || !checkSentinal(target) {
		// Nothing to unmount.
		klog.Infof("%s not found to unmount", target)
		return nil
	}
	// On any other error, we'll try to unmount and see what happens.
	_, umountErr := util.RunCommand(umountCmd, target)
	rmdirErr := os.Remove(target)
	sentinalErr := removeSentinal(target)
	if umountErr != nil {
		return fmt.Errorf("umount error %w; tried to rmdir and error was %v", umountErr, rmdirErr)
	}
	if rmdirErr != nil || sentinalErr != nil {
		klog.Warningf("Successfully unmounted peer at %s, but when removing mountpoint & sentinal got respectively %v & %v", target, rmdirErr, sentinalErr)
		// Return success anyway as we've disconnected the peer.
	}
	klog.Infof("unmount of %s completed", target)
	return nil
}

// fetchJobCoordinator fetches a non-empty coordinator IP for job. It can be canceled by the context.
func (r *replicationServer) fetchJobCoordinator(ctx context.Context, job string) (string, error) {
	configMap, err := r.queryCoordinatorConfigMap(ctx, job)
	if err != nil {
		return "", err
	}
	var ip string
	if configMap != nil && configMap.Data != nil {
		val, found := configMap.Data[coordinatorAddressKey]
		if found {
			ip = val
		}
	}
	for ip == "" && ctx.Err() == nil {
		if err := r.waitForWatcher(ctx); err != nil {
			return "", err
		}
		configMap, err = r.queryCoordinatorConfigMap(ctx, job)
		if err != nil {
			return "", err
		}
		if configMap != nil && configMap.Data != nil {
			val, found := configMap.Data[coordinatorAddressKey]
			if found {
				ip = val
			}
		}
	}
	return ip, ctx.Err()
}

// queryCoordinatorConfigMap looks for a job configmap and returns one, or nil if not found.
// An error is returned only on a non-retriable error (eg, timeout).
func (r *replicationServer) queryCoordinatorConfigMap(ctx context.Context, job string) (*corev1.ConfigMap, error) {
	configMap, err := r.configMapLister.ConfigMaps(r.opts.Namespace).Get(job)
	if k8serrors.IsNotFound(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return configMap, nil
}

func (r *replicationServer) waitForWatcher(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.watcherFlag:
	}
	return nil
}

func (r *replicationServer) raiseWatcherFlag() {
	select {
	case r.watcherFlag <- struct{}{}:
	default:
	}
}

func retryWithContext(ctx context.Context, operation func() error, msg string, retryInterval time.Duration) error {
	for {
		err := operation()
		if err == nil {
			return nil // Success
		}
		klog.Warningf("%s failed with error: %v, retrying...", msg, err)
		select {
		case <-ctx.Done():
			return buildCtxError(ctx)
		case <-time.After(retryInterval):
			// continue the loop
		}
	}
}

func buildCtxError(ctx context.Context) error {
	var err error
	switch ctx.Err() {
	case context.DeadlineExceeded:
		err = status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	case context.Canceled:
		err = status.Error(codes.Canceled, "context cancelled")
	default:
		err = status.Error(codes.Unknown, "unknown context done")
	}
	return err
}

func sentinalFor(target string) string {
	dir, file := filepath.Split(target)
	return filepath.Join(dir, sentinalPrefix+file)
}

func makeSentinal(target string) error {
	f, err := os.Create(sentinalFor(target))
	if err != nil {
		return err
	}
	f.Close()
	return nil
}

func checkSentinal(target string) bool {
	_, err := os.Stat(sentinalFor(target))
	return err == nil
}

func removeSentinal(target string) error {
	err := os.Remove(sentinalFor(target))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func getPeerMounts(dir string) ([]string, error) {
	entries, err := fs.ReadDir(os.DirFS(dir), ".")
	if err != nil {
		return nil, err
	}
	mountPoints := []string{}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), sentinalPrefix) {
			mountPoints = append(mountPoints, strings.Split(e.Name(), sentinalPrefix)[1])
		}
	}
	return mountPoints, nil
}
