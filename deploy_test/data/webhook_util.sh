#!/usr/bin/env bash
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# this example shows how to mount local ramdisk to a workload pod


set -o nounset -o errexit

key_size=4096
days=825
webhook_image=python:3.12.3-slim

overview() {
    cat <<EOF
USAGE

This script creates a webhook that forces scheduling of a jobset pods onto
specific nodes. It's based on very general webhook creation helpers from another
project. The webhook is installed in a fixed namespace (chkpt-webhook; the name
is short due to CA string length limits), but also includes cluster resources,
and so must be explicitly purged.

Since this is used for testing, the webhook is mandatory (if not, we can miss
pods. Since our setting the node name apparently overrides any pod
anti-affinity, this can lead to job pods being scheduled on the same node which
breaks our assumptions). This mandatory behavior is not suitable for a
production system, at least with this debug flask based webserver.

The target nodes are listed in order, so with a slice size of 2, the pod with
jobset indicies (0,0) goes to N1, (0,1) to N2, (1,0) to N3, etc. If not enough
nodes are specified the pod will not be forced to a node, but in practice this
leads to the job getting stuck as it's unlikely the scheduler assignment will
respect either where other pods have been placed, or the slice constraints.

The jobset base includes both the jobset and replica name, which means that the
generateName for the pods will look like jobset-base-2-3 for the pod in slice 2
and index 3.

Somewhat surprisingly, the webhook does not many resources, < 0.5 cpu and 200MiB
in practice

COMMAND LINE

$0 --jobset-base J --target-namespace N --target-nodes N1,N2... --slice-size K

or

$0 --purge

This will create a temp dir that is cleaned up on normal execution. Like the
rest of deploy_test, this uses gcloud and assume that location and cluster names
are set in gcloud config.

OVERVIEW

There are two parts in practice to defining a webhook. The first, easy, part is
to write a server that intercepts resource creation and mutates as
necessary. This is done via a python flask server in-line to a deployment. The
second and harder part is to authenticate the webhook to the API server. This
requires creating a certificate associated with the webhook server and
configuring the webhook with the provenance of the certificate.

There are in turn at least two ways to do this. The first uses a self-signed
certificate authority:

(1) Create a Certificate Authority (CA)
The CA is a public key pair that is used to sign a certificate for a
service. The public part is given as the "CA bundle" of the webhook
configuration. The K8s API server uses this to validate the certificate of a
webhook service which authenticates it as who you want to mutate your resource.

(2) Create a service certificate
A certificate combines an identity, a key, and a CA, to authenticate a
service. In our case the identity is the hostname of the webhook, eg
webhook.namespace.svc. The key is public key pair that is associated with the
webhook server. We sign the identity with this server key to create a
certificate signing request (CSR). This is then signed by the CA to create a
certificate.

(3) Deploy
The certificate and server key is placed into a K8s secret that is mounted into
the webhook server. The webhook uses this for TLS (ie, https). When the K8s API
servercalls the webhook, it validates the certificate given to it by the server
with the CA public key given to it as part of the webhook configuration. This
proves to the API server that the webhook endpoint is the right service and not
some other entity that's somehow gotten ahold of the service endpoint.

The alternative is to use the k8s api to sign a certificate. This gives less
control over the lifetime of the certificate (as if the cluster keys are
rotated, the certifacte becomes invalid). Conceptually this is simpler than the
above flow as there's no CA to create. In practice the service certificate
creation is more complicated.

(1) Create a webhook server key and CSR

(2) Create a k8s CSR using the certifactes.k8s.io API

(3) Deploy
The webhook creation now uses the k8s cluster config CA certificate for the
webhook CA bundle, and the certicate provided in the k8s CSR for the server
secret.

Note that a cluster can be configured differently in what sort of crypto is
accepts. For example, kind will accept RSA keys of 2048 bits, but GKE will
not. GKE will accept 4096 bit RSA keys, which is what we use here.

FILES

All files are created in the current directory ($(pwd)). When one action assumes
another, that means various files are assumed to exist. In the below, CA is used
for the value of --ca, SERVICE for --service, etc.

CA Files: CA.{key,pem}
    CA.pem is the public information used as the CA bundle in the webhook
    configuration. When the k8s CSR is used, there is no CA.key, and CA.pem is
    fetched from gcloud cluster configuration.

Cert files: SERVICE.{key,csr,ext,srl,crt,conf}
    SERVICE.crt is the certificate put in a secret to be used by the
    server. SERVICE.srl is the serial number of the certificate. SERVICE.ext
    specifies the identities (the hostname) signed into the certificate, and is
    used only with a self-signed CA. SERVICE.conf is similar but used only with
    the k8s CA.
EOF
}

get-cn() {
    echo "system:node:${service}.${namespace}.svc"
}

create-ca() {
    openssl genrsa -out "${ca}.key" "${key_size}"
    openssl req -x509 -new -key "${ca}.key" -days "${days}" -out "${ca}.pem" -subj /CN="$(get-cn)"
}

fetch-k8s-ca() {
    gcloud container clusters describe "$(gcloud config get container/cluster)" --format json \
      | jq --raw-output '.masterAuth.clusterCaCertificate | @base64d' > "${ca}.pem"
}

create-cert() {
    openssl genrsa -out "${service}.key" "${key_size}"
    openssl req -new -key "${service}.key" -out "${service}.csr" -subj /CN="$(get-cn)"
    echo "subjectAltName=DNS:${service}.${namespace}.svc, DNS:${service}.${namespace}, DNS:${service}" \
       > "${service}.ext"
    openssl x509 -req -in "${service}.csr" -CA "${ca}.pem" -CAkey "${ca}.key" -CAcreateserial \
       -days "${days}" -sha256 -extfile "${service}.ext" \
       -out "${service}.crt"
}

create-k8s-cert() {
    openssl genrsa -out "${service}.key" "${key_size}"
    cat <<EOF >> "${service}.conf"
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[ v3_req ]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = ${service}
DNS.2 = ${service}.${namespace}
DNS.3 = ${service}.${namespace}.svc
EOF
    openssl req -new -key "${service}.key" -subj /CN="$(get-cn)" -out "${service}.csr" -config "${service}.conf"

    # Deploy the CSR
    kubectl delete csr "${service}" 2>/dev/null || true
    cat <<EOF | kubectl create -f -
apiVersion: certificates.k8s.io/v1
kind: CertificateSigningRequest
metadata:
  name: ${service}
spec:
  groups:
  - system:authenticated
  request: $(cat "${service}.csr" | base64 | tr -d '\n')
  signerName: kubernetes.io/kubelet-serving
  usages:
  - digital signature
  - key encipherment
  - server auth
EOF
    until kubectl get csr "${service}"; do
        sleep 0.25
    done

    # Approve and fetch the signed cert
    kubectl certificate approve "${service}"
    start=$(date +%s)
    cert=''
    while [[ ${cert} == '' ]]; do
        if [[ $(($(date +%s) - $start)) -gt 60 ]]; then
            echo ERROR: timeout fetching signed server certificate 1>&2
            exit 1
        fi
        sleep 0.25
        cert="$(kubectl get csr ${service} -o jsonpath='{.status.certificate}')"
    done

    echo "${cert}" | openssl base64 -d -A -out "${service}.crt"
}

deploy-secret() {
    kubectl delete secret -n "${namespace}" "${service}-cert" 2>/dev/null || true
    kubectl create secret -n "${namespace}" generic "${service}-cert" \
      --from-file=key.pem="${service}.key" --from-file=cert.pem="${service}.crt"
}

indented-ca() {
    openssl base64 -in "${ca}.pem" | sed 's/^/        /'
}

# Writes webhook yaml to stdout. The first argument is inserted as the code
# block. It will be automatically indented. The code block runs in the mutate
# webhook handler. The input is in `request_data`, eg
# `request_data['request']['object']` will be the k8s resource.
create-webhook-yaml() {
    code_block="$(echo "${1}" | sed 's/^/                    /')"
    cat <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${service}
  namespace: ${namespace}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${service}
  template:
    metadata:
      labels:
        app: ${service}
    spec:
      containers:
        - name: ${service}
          image: ${webhook_image}
          imagePullPolicy: IfNotPresent
          command:
            - "/bin/sh"
            - "-c"
            - |
              pip3 install flask jsonpatch kubernetes jsonify

              cat > /webhook.py << EOF
              from flask import Flask, request, jsonify
              import jsonpatch
              from kubernetes import config
              import ssl, base64
              import logging.config

              logging.config.dictConfig({'version': 1, 'root': {'level': 'INFO'}})
              app = Flask(__name__)
              context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
              context.load_cert_chain('/etc/tls-certs/cert.pem', '/etc/tls-certs/key.pem')

              # Load Kubernetes configuration
              config.load_incluster_config()

              def admission_response_patch(uid, message, json_patch):
                  base64_patch = base64.b64encode(json_patch.to_string().encode('utf-8')).decode('utf-8')
                  return jsonify({'response': {'allowed': True,
                                              'uid': uid,
                                              'status': {'message': message},
                                              'patchType': 'JSONPatch',
                                              'patch': base64_patch},
                                  'apiVersion': 'admission.k8s.io/v1',
                                  'kind': 'AdmissionReview'})

              @app.route('/mutate', methods=['POST'])
              def mutate():
                try:
${code_block}
                except Exception as exc:
                  app.logger.error(f'processing error, no change done: {repr(exc)} on {request_data['request']['object']}')
                  return admission_response_patch(uid, "no changes", json_patch)
              if __name__ == '__main__':
                  app.run(host='0.0.0.0', port=8080, ssl_context=context)
              EOF

              python /webhook.py
          resources:
            requests:
              cpu: 1
              memory: 500Mi
          ports:
            - name: mutate
              containerPort: 8080
          volumeMounts:
            - name: certs
              mountPath: /etc/tls-certs
              readOnly: true
      volumes:
        - name: certs
          secret:
            secretName: ${service}-cert
---
apiVersion: v1
kind: Service
metadata:
  name: ${service}
  namespace: ${namespace}
spec:
  selector:
    app: ${service}
  ports:
    - name: mutate
      protocol: TCP
      port: 443
      targetPort: 8080
---
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: ${service}
webhooks:
  - name: ${service}.local.gke.io
    clientConfig:
      caBundle: |
$(indented-ca)
      service:
        name: ${service}
        namespace: ${namespace}
        path: "/mutate"
    rules:
      - operations: [${operations}]
        apiGroups: ["${gvk_group:-}"]
        apiVersions: ["${gvk_version}"]
        resources: ["${gvk_kind}"]
        scope: "Namespaced"
    failurePolicy: Fail
    admissionReviewVersions: ["v1"]
    sideEffects: None
    reinvocationPolicy: Never
    timeoutSeconds: 3
EOF
}

purge-webhook() {
    kubectl delete CertificateSigningRequest ${service} 2>/dev/null || true
    kubectl delete Deployment ${service} -n ${namespace} 2>/dev/null || true
    kubectl delete Service ${service} -n ${namespace} 2>/dev/null || true
    kubectl delete MutatingWebhookConfiguration ${service} 2>/dev/null || true
    kubectl delete Secret ${service}-cert -n ${namespace}  2>/dev/null || true
    kubectl delete namespace ${namespace} 2>/dev/null || true
}

purge_only=no
count=0
while [[ $# -gt 0 ]]; do
    let count=count+1
    case ${1} in
        --help|-h)
            overview
            exit 1
            ;;
        --jobset-base)
            jobset_base=$2
            shift
            ;;
        --target-namespace)
            target_namespace=$2
            shift
            ;;
        --slice-size)
            slice_size=$2
            shift
            ;;
        --target-nodes)
            target_nodes=$2
            shift
            ;;
        --purge)
            purge_only=yes
            ;;
        *)
            echo "unknown argument $1 (see -h for details)"
            exit 1
            ;;
    esac
    shift
done

# note that names must be short due to CA limits.
namespace=chkpt-webhook
ca=chkpt-ca
service=chkpt-webhook
gvk_version=v1
gvk_kind=pods
operations='"CREATE"'

if [ ${purge_only} = yes -a ${count} != 1 ]; then
    echo "the --purge flag can't be used with other arguments"
    exit 1
fi

echo cleaning up any existing webhook...
purge-webhook

if [ ${purge_only} = yes ]; then
    echo purge complete, exiting.
    exit 0
fi

tmpdir=$(mktemp -d)
echo using ${tmpdir}
pushd "${tmpdir}"

kubectl create namespace ${namespace}

echo creating self-signed ${ca}...
create-ca

echo creating cert for ${service}...
create-cert

echo deploying secret...
deploy-secret

echo deploying webhook...

code="
request_data = request.get_json()
uid = request_data['request']['uid']
pod = request_data['request']['object']
json_patch = jsonpatch.JsonPatch([])

if 'metadata' not in pod:
    return admission_response_patch(uid, 'no changes', json_patch)
pod_metadata = pod['metadata']

for f in ('namespace',
          'generateName'):
    if f not in pod_metadata:
        return admission_response_patch(uid, 'no changes', json_patch)

if pod_metadata['namespace'] != '${target_namespace}':
    return admission_response_patch(uid, 'no changes', json_patch)

gen_name = pod_metadata['generateName']
app.logger.info(f'considering {gen_name}')
if not pod_metadata['generateName'].startswith('${jobset_base}-'):
    app.logger.info(f'skipping {gen_name}')
    return admission_response_patch(uid, 'no changes', json_patch)

nodes = '${target_nodes}'.split(',')

import re
# Extra slashes as this is interpreted by the shell.
pattern = r'${jobset_base}-(\\d+)-(\\d+)'
m = re.match(pattern, gen_name)
if not m:
    app.logger.error(f'no match for {gen_name} with {pattern}')
    return admission_response_patch(uid, 'no changes', json_patch)

slice = int(m.group(1))
idx = int(m.group(2))
rank = slice*${slice_size} + idx
if rank >= len(nodes):
    app.logger.error(f'rank {rank} too big for The {len(nodes)} we know about')
    return admission_response_patch(uid, 'no changes', json_patch)

json_patch.patch.append({'op': 'replace', 'path': f'/spec/nodeName', 'value': nodes[rank]})
app.logger.info(f'Updated {gen_name} to schedule to {nodes[rank]}')
return admission_response_patch(uid, 'modified PD CSI Node server memory limit', json_patch)
"
create-webhook-yaml "${code}" | kubectl apply -f -

echo cleaning up tempdir...
popd
rm -rv "${tmpdir}"

echo waiting for webhook to start...
while kubectl get pod -n "${namespace}" -l "app=${service}" --no-headers | grep -vq Running; do
    sleep 5
    echo webhook not yet up...
done
until kubectl logs -n ${namespace} -l "app=${service}" --tail 20 | egrep -q 'Running on all addresses|POST /mutate'; do
  echo webhook running but installing, which can take several minutes...
  sleep 5
done

echo success!
