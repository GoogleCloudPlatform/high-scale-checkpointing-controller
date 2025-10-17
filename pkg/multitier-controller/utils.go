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

package multitiercontroller

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	volumehelpers "k8s.io/cloud-provider/volume/helpers"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	checkpoint "gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/apis/checkpointing.gke.io/v1"
	"gke-internal.googlesource.com/gke-storage/high-scale-checkpointing/pkg/util"
)

const (
	remoteVolumeName   = "backup"
	peerVolumeName     = "peers"
	localVolumeName    = "local"
	hugePageConfigName = "sys-mm"

	metricsCollectorContainerName       = "checkpointing-metrics-collector"
	customMapNameField                  = "custom-image-config-name"
	defaultCustomMapName                = "custom-checkpoint-image-config"
	metricsCollectorConfigMapVolumeName = "checkpointing-metrics-collector-config-map-vol"
	tokenBrokerVolumeName               = "token-broker-ksa"
	tokenBrokerADC                      = "token-broker-adc"

	gkeComponentName = "highscalecheckpointing"
)

type preparer struct {
	config        *csiconfig
	cpc           *checkpoint.CheckpointConfiguration
	metricsConfig *MetricsCollectorConfig
	tmpfsSizeMib  string

	registrarImage         string
	csiImage               string
	replicationWorkerImage string
	nfsServerImage         string
	metricsCollectorImage  string
	tokenBrokerImage       string
}

func newPreparer(ctx context.Context, k8sClient *kubernetes.Clientset, config *csiconfig, cpc *checkpoint.CheckpointConfiguration, metricsiconfig *MetricsCollectorConfig) (*preparer, error) {
	if cpc == nil {
		return nil, fmt.Errorf("Cannot prepare with null CPC")
	}

	if cpc.Spec.CloudStorageBucketName == "" {
		return nil, fmt.Errorf("cloudStorageBucketName need to be specified in the checkpointconfiguration")
	}
	tmpfsSizeMib, err := convertQuantityToInt64(cpc.Spec.InMemoryVolumeSize)
	if err != nil {
		return nil, fmt.Errorf("invalid inMemoryVolumeSize format:  %+v", err)
	}

	p := &preparer{
		config:        config,
		cpc:           cpc,
		tmpfsSizeMib:  strconv.FormatInt(tmpfsSizeMib, 10),
		metricsConfig: metricsiconfig,
	}

	if cpc.Spec.CsiEphemeralLimit != "" {
		if _, err := resource.ParseQuantity(p.cpc.Spec.CsiEphemeralLimit); err != nil {
			return nil, fmt.Errorf("bad csi ephemeral limit %s: %w", p.cpc.Spec.CsiEphemeralLimit, err)
		}
	}

	// Load container images from ConfigMap
	imageMap, err := k8sClient.CoreV1().ConfigMaps(config.csiNamespace).Get(ctx, config.imageConfigMapName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get configmap for container images: %w", err)
	}
	if imageMap.Data == nil {
		return nil, fmt.Errorf("missing data in image map: %v", imageMap)
	}
	var customMap *corev1.ConfigMap
	foundCustomMap := false
	customName := imageMap.Data[customMapNameField]
	if customName == "" {
		customName = defaultCustomMapName
	}

	customMap, err = k8sClient.CoreV1().ConfigMaps(config.csiNamespace).Get(ctx, customName, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return nil, fmt.Errorf("error fetching custom image map: %w", err)
	}
	if err == nil && customMap.Data != nil {
		foundCustomMap = true
	}

	if foundCustomMap {
		log.FromContext(ctx).Info("overriding images", "custom map", customMap.Data)
		p.registrarImage = customMap.Data["registrar-image"]
		p.csiImage = customMap.Data["csi-image"]
		p.replicationWorkerImage = customMap.Data["replication-worker-image"]
		p.nfsServerImage = customMap.Data["nfs-server-image"]
	}
	if p.registrarImage == "" {
		p.registrarImage = imageMap.Data["registrar-image"]
	}
	if p.csiImage == "" {
		p.csiImage = imageMap.Data["csi-image"]
	}
	if p.replicationWorkerImage == "" {
		p.replicationWorkerImage = imageMap.Data["replication-worker-image"]
	}
	if p.nfsServerImage == "" {
		p.nfsServerImage = imageMap.Data["nfs-server-image"]
	}
	p.metricsCollectorImage = imageMap.Data["metrics-collector-image"]
	p.tokenBrokerImage = imageMap.Data["token-broker-image"]

	if p.registrarImage == "" || p.csiImage == "" || p.replicationWorkerImage == "" || p.nfsServerImage == "" {
		return nil, fmt.Errorf("missing image: %+v", p)
	}

	if p.metricsConfig != nil && (p.metricsCollectorImage == "" || p.tokenBrokerImage == "") {
		return nil, fmt.Errorf("node metrics collection is on but missing metrics-collector-image or token-broker-image")
	}

	return p, nil
}

func (p *preparer) prepareCSIDaemonSet(dsName string) v1.DaemonSet {
	csi := v1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dsName,
			Namespace: p.config.csiNamespace,
			Annotations: map[string]string{
				"components.gke.io/layer": "addon",
			},
			Labels: map[string]string{
				"k8s-app":                         "high-scale-checkpointing",
				"addonmanager.kubernetes.io/mode": "Reconcile",
			},
		},
		Spec: v1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"k8s-app": "high-scale-checkpointing"},
			},
			Template: p.preparePodTemplateSpec(),
		},
	}

	if p.cpc.Spec.NodeSelector != nil {
		if csi.Spec.Template.Spec.NodeSelector == nil {
			csi.Spec.Template.Spec.NodeSelector = make(map[string]string)
		}

		for k, v := range p.cpc.Spec.NodeSelector {
			csi.Spec.Template.Spec.NodeSelector[k] = v
		}
	}

	if p.cpc.Spec.Tolerations != nil {
		if csi.Spec.Template.Spec.Tolerations == nil {
			csi.Spec.Template.Spec.Tolerations = []corev1.Toleration{}
		}
		csi.Spec.Template.Spec.Tolerations = append(csi.Spec.Template.Spec.Tolerations, p.cpc.Spec.Tolerations...)
	}

	return csi
}

func (p *preparer) preparePodTemplateSpec() corev1.PodTemplateSpec {
	ephemeralLimit := "0"
	if p.cpc.Spec.CsiEphemeralLimit != "" {
		ephemeralLimit = p.cpc.Spec.CsiEphemeralLimit
	}
	initContainers := []corev1.Container{}

	containerSpecs := []corev1.Container{
		p.prepareRegistrarContainer(),
		p.prepareCSIContainer(),
		p.prepareReplicationWorkerContainer(),
		p.prepareNFSServerContainer(),
	}
	if p.metricsConfig != nil {
		klog.Infof("node metrics collection is on, adding metric collector sidecars")
		klog.Infof("metrics collector config: %+v", p.metricsConfig)

		containerSpecs = append(containerSpecs, p.prepareMetricsCollectorContainer())
		// NodeP4SA integration
		initContainers = append(initContainers, p.prepareTokenBrokerInitContainer())
	}

	podTemplate := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"k8s-app": "high-scale-checkpointing"},
			Annotations: map[string]string{
				"seccomp.security.alpha.kubernetes.io/pod": "runtime/default",
				"gke-gcsfuse/volumes":                      "true",
				"gke-gcsfuse/cpu-limit":                    "0",
				"gke-gcsfuse/memory-limit":                 "0",
				"gke-gcsfuse/ephemeral-storage-limit":      ephemeralLimit,
			},
		},
		Spec: corev1.PodSpec{
			PriorityClassName: "system-node-critical",
			SecurityContext: &corev1.PodSecurityContext{
				SeccompProfile: &corev1.SeccompProfile{
					Type: corev1.SeccompProfileTypeRuntimeDefault,
				},
			},
			ServiceAccountName: p.config.csiServiceAccountName,
			NodeSelector:       map[string]string{"kubernetes.io/os": "linux"},
			InitContainers:     initContainers,
			Containers:         containerSpecs,
			Volumes:            p.prepareVolume(),
		},
	}
	return podTemplate
}

func (p *preparer) prepareRegistrarContainer() corev1.Container {
	return corev1.Container{
		Name:  "registrar",
		Image: p.registrarImage,
		Args: []string{
			"--v=5",
			"--csi-address=/csi/csi.sock",
			fmt.Sprintf("--kubelet-registration-path=/var/lib/kubelet/plugins/%s/csi.sock", p.config.csiDriverName),
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("5m"),
				corev1.ResourceMemory: resource.MustParse("10Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("50Mi"),
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "plugin-dir",
				MountPath: "/csi",
			},
			{
				Name:      "registration-dir",
				MountPath: "/registration",
			},
		},
	}

}

func (p *preparer) prepareCSIContainer() corev1.Container {
	container := corev1.Container{
		Name:  "csi",
		Image: p.csiImage,
		Args: []string{
			"--v=5",
			"--endpoint=unix:/csi/csi.sock",
			fmt.Sprintf("--driver-name=%s", p.config.csiDriverName),
			"--namespace=$(NAMESPACE)",
			"--csi-node-id=$(NODE_NAME)",
			fmt.Sprintf("--cpc-name=%s", p.cpc.GetName()),
			"--peer-dir=/peers",
			"--remote-dir=/remote/gcs",
			"--local-dir=/local",
			// This is shared with the replication worker container
			"--replication-port=2112",
			"--metrics-address=:20062", // TODO: make this configurable
			// Jax process id mapping
			"--jax-id-mapping=true",
			"--persistent-dir=/persistent",
			"--nfs-export=/exports/client",
			fmt.Sprintf("--tmpfs-size-mib=%s", p.tmpfsSizeMib),
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("10m"),
			},
		},
		Env: []corev1.EnvVar{
			{
				Name: "NODE_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "spec.nodeName",
					},
				},
			},
			{
				Name: "NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
			{
				Name:  "REMOTE_DIR",
				Value: "/remote/gcs",
			},
		},
		SecurityContext: &corev1.SecurityContext{
			Privileged: ptr.To(true),
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:             "kubelet-pods-dir",
				MountPath:        "/var/lib/kubelet/pods",
				MountPropagation: ptr.To(corev1.MountPropagationBidirectional),
			},
			{
				Name:      "plugin-dir",
				MountPath: "/csi",
			},
			{
				Name:      peerVolumeName,
				MountPath: "/peers",
				// mountPropagation is needed for the NFS mounts created here.
				MountPropagation: ptr.To(corev1.MountPropagationBidirectional),
			},
			{
				Name:             remoteVolumeName,
				MountPath:        "/remote",
				MountPropagation: ptr.To(corev1.MountPropagationBidirectional),
			},
			{
				Name:             localVolumeName,
				MountPath:        "/local/tmpfs",
				MountPropagation: ptr.To(corev1.MountPropagationBidirectional),
			},
			{
				Name:      gcsVolumeName,
				MountPath: "/persistent",
			},
			{
				Name:      hugePageConfigName,
				MountPath: "/sys/kernel/mm",
			},
		},
	}

	if !p.config.legacyIdFile {
		// TODO: this is hardcoded to match the high-scale-checkpointing-controller
		// service defined in deploy/multitier/controller/controller.yaml. That
		// service name should be plumbed in through a flag. Note that it shouldn't
		// come from the checkpoint configuration CRD, as the service name is not
		// user controlled.
		container.Args = append(container.Args, fmt.Sprintf("--ranks-server-target=high-scale-checkpointing-controller:%d", p.config.ranksServerPort))
	}

	if p.config.localDeploy {
		container.ImagePullPolicy = corev1.PullAlways
	}

	return container
}

func (p *preparer) prepareReplicationWorkerContainer() corev1.Container {
	container := corev1.Container{
		Name:  "replication-worker",
		Image: p.replicationWorkerImage,
		Env: []corev1.EnvVar{
			{
				Name: "POD_IP",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "status.podIP",
					},
				},
			},
			{
				Name:  "PLATFORM_API",
				Value: "GKE_MANAGED",
			},
			{
				Name:  "LOCAL_VOLUME",
				Value: "local/client",
			},
			{
				Name:  "BACKUP_VOLUME",
				Value: "backup/gcs",
			},
		},
		SecurityContext: &corev1.SecurityContext{
			Privileged: ptr.To(true),
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("10m"),
			},
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          "coordinator",
				ContainerPort: 4242,
			},
			// TODO: make this configurable
			{
				Name:          "prometheus",
				ContainerPort: 8000,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:             peerVolumeName,
				MountPath:        "/app/repl",
				MountPropagation: ptr.To(corev1.MountPropagationBidirectional),
			},
			{
				Name:             remoteVolumeName,
				MountPath:        "/app/backup",
				MountPropagation: ptr.To(corev1.MountPropagationBidirectional),
			},
			{
				Name:             localVolumeName,
				MountPath:        "/app/local",
				MountPropagation: ptr.To(corev1.MountPropagationBidirectional),
			},
		},
	}

	if p.config.localDeploy {
		container.ImagePullPolicy = corev1.PullAlways
	}

	if p.config.debugBackup {
		container.Env = append(container.Env, corev1.EnvVar{Name: "DEBUG_BACKUP", Value: "true"})
	}

	return container
}

func (p *preparer) prepareNFSServerContainer() corev1.Container {
	container := corev1.Container{
		Name:  "nfs-server",
		Image: p.nfsServerImage,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("200Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("800Mi"),
			},
		},
		Env: []corev1.EnvVar{
			{
				Name:  "SHARED_DIRECTORY",
				Value: "/exports",
			},
		},
		SecurityContext: &corev1.SecurityContext{
			Privileged: ptr.To(true),
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          "tcp-2049",
				ContainerPort: 2049,
			},
			{
				Name:          "mountd",
				ContainerPort: 20048,
			},
			{
				Name:          "sunrpc",
				ContainerPort: 111,
			},
			{
				Name:          "nlm",
				ContainerPort: 4045,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "modules",
				MountPath: "/lib/modules",
			},
			{
				Name:             localVolumeName,
				MountPath:        "/exports",
				MountPropagation: ptr.To(corev1.MountPropagationBidirectional),
			},
		},
	}

	if p.config.localDeploy {
		container.ImagePullPolicy = corev1.PullAlways
	}

	return container
}

func (p *preparer) prepareTokenBrokerInitContainer() corev1.Container {
	container := corev1.Container{
		Name:  "token-broker-adc-init",
		Image: p.tokenBrokerImage,
		Command: []string{
			"/node_token_broker_init",
			fmt.Sprintf("--audience=%s", p.metricsConfig.Audience),
			fmt.Sprintf("--token_url=%s", p.metricsConfig.TokenURL),
			fmt.Sprintf("--token_file=%s", p.metricsConfig.KSAFilePath),
			fmt.Sprintf("--output_path=%s", p.metricsConfig.ADCFilePath),
			fmt.Sprintf("--project_id=%s", p.metricsConfig.ConsumerProjectID),
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      tokenBrokerADC,
				MountPath: p.metricsConfig.ADCVolumeMountDir,
				ReadOnly:  false,
			},
		},
	}
	return container
}

func (p *preparer) prepareMetricsCollectorContainer() corev1.Container {
	container := corev1.Container{
		Name:  metricsCollectorContainerName,
		Image: p.metricsCollectorImage,
		SecurityContext: &corev1.SecurityContext{
			ReadOnlyRootFilesystem:   ptr.To(true),
			AllowPrivilegeEscalation: ptr.To(false),
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
			},
			RunAsUser:  ptr.To(int64(1000)),
			RunAsGroup: ptr.To(int64(1000)),
			SeccompProfile: &corev1.SeccompProfile{
				Type: corev1.SeccompProfileTypeRuntimeDefault,
			},
		},
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("45Mi"),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("45Mi"),
			},
		},
		Env: []corev1.EnvVar{
			{
				Name:  "COLLECTOR_CONFIG_PATH",
				Value: "/conf/replication-worker-metrics-collector-config-data.textproto,/conf/csi-metrics-collector-config-data.textproto",
			},
			{
				Name:  "SPLIT_GAUGE_BUFFER",
				Value: "true",
			},
			{
				Name:  "PROJECT_NUMBER",
				Value: p.metricsConfig.ProjectNumber,
			},
			{
				Name:  "LOCATION",
				Value: p.metricsConfig.ClusterLocation,
			},
			{
				Name:  "CLUSTER_NAME",
				Value: p.metricsConfig.ClusterName,
			},
			{
				Name: "POD_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
			{
				Name: "NODE_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "spec.nodeName",
					},
				},
			},
			{
				Name: "POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				},
			},
			{
				Name:  "CONTAINER_NAME",
				Value: metricsCollectorContainerName,
			},
			{
				Name: "KUBELET_HOST",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "status.hostIP",
					},
				},
			},
			{
				Name:  "COMPONENT_VERSION",
				Value: util.GetEnvVar(util.EnvGKEComponentVersion),
			},
			{
				Name:  "COMPONENT_NAME",
				Value: gkeComponentName,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      metricsCollectorConfigMapVolumeName,
				MountPath: "/conf",
			},
		},
	}

	// NodeP4SA integration
	container.VolumeMounts = append(container.VolumeMounts, []corev1.VolumeMount{
		{
			Name:      tokenBrokerVolumeName,
			MountPath: p.metricsConfig.KSAVolumeMountDir,
			ReadOnly:  true,
		},
		{
			Name:      tokenBrokerADC,
			MountPath: p.metricsConfig.ADCVolumeMountDir,
			ReadOnly:  true,
		},
	}...)
	container.Env = append(container.Env, []corev1.EnvVar{
		{
			Name:  "TOKEN_SOURCE_MODE",
			Value: p.metricsConfig.TokenMode,
		},
		{
			Name:  "GKE_NODE_SYSTEM_WORKLOAD_CREDENTIALS",
			Value: p.metricsConfig.ADCFilePath,
		},
		{
			Name:  "GKE_HOSTNAME",
			Value: p.metricsConfig.GKEHostName,
		},
	}...)

	return container
}

func (p *preparer) prepareVolume() []corev1.Volume {
	volumes := []corev1.Volume{
		{
			Name: "registration-dir",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/lib/kubelet/plugins_registry/",
					Type: ptr.To(corev1.HostPathDirectory),
				},
			},
		},
		{
			Name: "kubelet-pods-dir",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/lib/kubelet/pods",
					Type: ptr.To(corev1.HostPathDirectory),
				},
			},
		},
		{
			Name: hugePageConfigName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/sys/kernel/mm",
					Type: ptr.To(corev1.HostPathDirectory),
				},
			},
		},
		{
			Name: "plugin-dir",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: fmt.Sprintf("/var/lib/kubelet/plugins/%s", p.config.csiDriverName),
					Type: ptr.To(corev1.HostPathDirectoryOrCreate),
				},
			},
		},
		{
			Name: "modules",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/lib/modules",
					Type: ptr.To(corev1.HostPathDirectory),
				},
			},
		},
		{
			Name: peerVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium: corev1.StorageMediumMemory,
				},
			},
		},
		{
			Name: localVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium: corev1.StorageMediumMemory,
				},
			},
		},
		{
			Name: remoteVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium: corev1.StorageMediumMemory,
				},
			},
		},
		{
			Name: gcsVolumeName,
			VolumeSource: corev1.VolumeSource{
				CSI: &corev1.CSIVolumeSource{
					Driver: "gcsfuse.csi.storage.gke.io",
					VolumeAttributes: map[string]string{
						"bucketName":               p.cpc.Spec.CloudStorageBucketName,
						"skipCSIBucketAccessCheck": "true",
						"mountOptions":             strings.Join(p.cpc.Spec.GcsFuseMountOptions, ","),
					},
				},
			},
		},
	}

	if p.metricsConfig != nil {
		volumes = append(volumes, corev1.Volume{
			Name: metricsCollectorConfigMapVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: "checkpointing-metrics-collector-config-map",
					},
					Items: []corev1.KeyToPath{
						{
							Key:  "replication-worker-metrics-collector-config-data",
							Path: "replication-worker-metrics-collector-config-data.textproto",
						},
						{
							Key:  "csi-metrics-collector-config-data",
							Path: "csi-metrics-collector-config-data.textproto",
						},
					},
				},
			},
		})

		// NodeP4SA integration
		brokerVolumes := []corev1.Volume{
			{
				Name: tokenBrokerVolumeName,
				VolumeSource: corev1.VolumeSource{
					Projected: &corev1.ProjectedVolumeSource{
						Sources: []corev1.VolumeProjection{
							{
								ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
									Path:              p.metricsConfig.KSATokenFileName,
									Audience:          p.metricsConfig.Audience,
									ExpirationSeconds: ptr.To(int64(3600)),
								},
							},
						},
					},
				},
			},
			{
				Name: tokenBrokerADC,
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		}
		volumes = append(volumes, brokerVolumes...)
	}

	return volumes
}

// convertQuantityToInt64 converts the input size string to the equivalent value in MiB as an int64
func convertQuantityToInt64(str string) (int64, error) {
	quantity, err := resource.ParseQuantity(str)
	if err != nil {
		return -1, err
	}
	return volumehelpers.RoundUpToMiB(quantity)
}

// uptimeReconcilerPodTrimmer removes unnecessary fields from the Pod object before admitting into informer cache.
func uptimeReconcilerPodTrimmer(obj interface{}) (interface{}, error) {
	podObj, ok := obj.(*corev1.Pod)
	if !ok {
		// Not a pod, return the original object.
		return obj.(runtime.Object), nil
	}

	// Create a new pod object with only the pod status fields.
	trimmedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            podObj.Name,
			Namespace:       podObj.Namespace,
			OwnerReferences: podObj.OwnerReferences,
		},
		Spec: corev1.PodSpec{
			// The node name is used to detect conflict between CheckpointConfigurations.
			NodeName: podObj.Spec.NodeName,
		},
		Status: corev1.PodStatus{
			Conditions: podObj.Status.Conditions,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
	}

	return trimmedPod, nil
}
