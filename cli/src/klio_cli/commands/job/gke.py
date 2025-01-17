# Copyright 2021 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import os
import re

import glom
import yaml
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

from klio_cli import __version__ as klio_cli_version
from klio_cli.commands import base
from klio_cli.utils import docker_utils


# Regex according to https://kubernetes.io/docs/concepts/overview/
# working-with-objects/labels/#syntax-and-character-set
K8S_LABEL_KEY_PREFIX_REGEX = re.compile(
    r"^([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])"
    r"(\.([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9]))*$"
)
K8S_LABEL_KEY_NAME_REGEX = re.compile(
    r"^[a-zA-Z0-9]$|^[a-zA-Z0-9]([a-zA-Z0-9\._\-]){,61}[a-zA-Z0-9]$"
)
K8S_LABEL_VALUE_REGEX = re.compile(
    r"^[a-zA-Z0-9]{0,1}$|^[a-zA-Z0-9]([a-zA-Z0-9\._\-]){,61}[a-zA-Z0-9]$"
)
K8S_RESERVED_KEY_PREFIXES = ("kubernetes.io", "k8s.io")


class GKECommandMixin(object):
    # NOTE : This command requires a job_dir attribute

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._deployment_config = None
        self._kubernetes_client = None
        self._kubernetes_active_context = None

    @property
    def kubernetes_client(self):
        if not self._kubernetes_client:
            # TODO: This grabs configs from '~/.kube/config'. @shireenk
            #  We should add a check that this file exists
            # If it does not exist then we should create configurations.
            # See link:
            # https://github.com/kubernetes-client/python-base/blob/master/config/kube_config.py#L825
            k8s_config.load_kube_config()
            self._kubernetes_client = k8s_client.AppsV1Api()

        return self._kubernetes_client

    @property
    def kubernetes_active_context(self):
        if not self._kubernetes_active_context:
            _, active_context = k8s_config.list_kube_config_contexts()
            self._kubernetes_active_context = active_context

        return self._kubernetes_active_context

    @property
    def deployment_config(self):
        if not self._deployment_config:
            path_to_deployment_config = os.path.join(
                self.job_dir, "kubernetes", "deployment.yaml"
            )
            with open(path_to_deployment_config) as f:
                self._deployment_config = yaml.safe_load(f)
        return self._deployment_config

    def _deployment_exists(self):
        """Check to see if a deployment already exists

        Returns:
            bool: Whether a deployment for the given name-namespace
                combination exists
        """
        dep = self.deployment_config
        namespace = glom.glom(dep, "metadata.namespace")
        deployment_name = glom.glom(dep, "metadata.name")
        resp = self.kubernetes_client.list_namespaced_deployment(
            namespace=namespace,
        )
        for i in resp.items:
            if i.metadata.name == deployment_name:
                return True
        return False

    def _update_deployment(self, replica_count=None, image_tag=None):
        """This will update a deployment with a provided
            replica count or image tag

        Args:
            replica_count (int): Number of replicas the
                deployment will be updated with.
                If not provided then this will not be changed
            image_tag (str): The image tag that will be applied
                to the updated deployment.
                If not provided then this will not be updated.
        """
        deployment_name = glom.glom(self.deployment_config, "metadata.name")
        namespace = glom.glom(self.deployment_config, "metadata.namespace")
        log_messages = []
        if replica_count is not None:
            glom.assign(
                self._deployment_config, "spec.replicas", replica_count
            )
            log_messages.append(f"Scaled deployment to {replica_count}")
        if image_tag:
            image_path = "spec.template.spec.containers.0.image"
            image_base = glom.glom(self._deployment_config, image_path)
            # Strip off existing image tag if present
            image_base = re.split(":", image_base)[0]
            full_image = image_base + f":{image_tag}"
            glom.assign(self._deployment_config, image_path, full_image)
            log_messages.append(
                f"Update deployment with image tag {image_tag}"
            )
        resp = self.kubernetes_client.patch_namespaced_deployment(
            name=deployment_name,
            namespace=namespace,
            body=self.deployment_config,
        )
        log_messages.append(f"Update deployment with {resp.metadata.name}")
        for message in log_messages:
            logging.info(message)


class RunPipelineGKE(GKECommandMixin, base.BaseDockerizedPipeline):
    def __init__(
        self, job_dir, klio_config, docker_runtime_config, run_job_config
    ):
        super().__init__(job_dir, klio_config, docker_runtime_config)
        self.run_job_config = run_job_config

    def _apply_image_to_deployment_config(self):
        image_tag = self.docker_runtime_config.image_tag
        pipeline_options = self.klio_config.pipeline_options
        if image_tag:
            dep = self.deployment_config
            image_path = "spec.template.spec.containers.0.image"
            # TODO: If more than one image deployed,
            #  we need to search for correct container
            image_base = glom.glom(dep, image_path)
            # Strip off existing image tag if any
            image_base = re.split(":", image_base)[0]
            full_image = f"{image_base}:{image_tag}"
            glom.assign(self._deployment_config, image_path, full_image)
        # Check to see if the kubernetes image to be deployed is the same
        # image that is built
        k8s_image = glom.glom(self.deployment_config, image_path)
        built_image_base = pipeline_options.worker_harness_container_image
        built_image = f"{built_image_base}:{image_tag}"
        if built_image != k8s_image:
            logging.warning(
                f"Image deployed by kubernetes {k8s_image} does not match "
                f"the built image {built_image}. "
                "This may result in an `ImagePullBackoff` for the deployment. "
                "If this is not intended, please change "
                "`pipeline_options.worker_harness_container_image` "
                "and rebuild  or change the container image"
                "set in kubernetes/deployment.yaml file."
            )

    @staticmethod
    def _validate_labels(label_path, label_dict):
        help_url = (
            "https://kubernetes.io/docs/concepts/overview/working-with-objects"
            "/labels/#syntax-and-character-set"
        )
        for key, value in label_dict.items():
            # Both key and value must be strings
            if not isinstance(key, str):
                raise ValueError(f"Key '{label_path}.{key}' must be a string.")
            if not isinstance(value, str):
                raise ValueError(
                    f"Value '{value}' for key '{label_path}.{key}' must be a "
                    "string"
                )

            # Handle any prefixes in keys
            if "/" in key:
                # validate that there's at most one forward slash
                prefix, *name = key.split("/")
                if len(name) > 1:
                    raise ValueError(
                        f"Unsupported key name in {label_path}: '{key}' "
                        f"contains more than one forward slash. See {help_url} "
                        "for valid label keys."
                    )

                # validate prefix
                prefix_match = K8S_LABEL_KEY_PREFIX_REGEX.match(prefix)
                if (
                    prefix_match is None
                    or prefix in K8S_RESERVED_KEY_PREFIXES
                    or len(prefix) > 253
                ):
                    raise ValueError(
                        f"Unsupported prefix key name in {label_path}: "
                        f"'{prefix}'. See {help_url} for valid label key "
                        "prefixes."
                    )
                key = name[0]

            # Validate the key
            key_match = K8S_LABEL_KEY_NAME_REGEX.match(key)
            if key_match is None:
                raise ValueError(
                    f"Unsupported key name in {label_path}: '{key}'. "
                    f"See {help_url} for valid label keys."
                )

            # Validate the value
            value_match = K8S_LABEL_VALUE_REGEX.match(value)
            if not value_match:
                raise ValueError(
                    f"Unsupported value '{value}' for '{label_path}.{key}'. "
                    f"See {help_url} for valid values."
                )

    def _apply_labels_to_deployment_config(self):
        # `metadata.labels` are a best practices thing, but not required
        # (these would be "deployment labels"). At least one label defined in
        # `spec.template.metadata.labels` is required for k8s deployments
        # ("pod labels").
        # There also must be at least one "selector label"
        # (`spec.selector.matchLabels`) which connects the deployment to pod.
        # More info: https://stackoverflow.com/a/54854179
        # TODO: add environment labels if/when we support dev/test/prod envs
        metadata_labels, pod_labels, selector_labels = {}, {}, {}

        # standard practice labels ("app" and "role")
        existing_metadata_labels = glom.glom(
            self.deployment_config, "metadata.labels", default={}
        )
        metadata_app = glom.glom(existing_metadata_labels, "app", default=None)
        if not metadata_app:
            job_name = self.klio_config.job_name
            metadata_labels["app"] = job_name
        metadata_labels.update(existing_metadata_labels)

        existing_pod_labels = glom.glom(
            self.deployment_config, "spec.template.metadata.labels", default={}
        )
        pod_app = glom.glom(existing_pod_labels, "app", default=None)
        pod_role = glom.glom(existing_pod_labels, "role", default=None)
        if not pod_app:
            pod_app = metadata_labels["app"]
        if not pod_role:
            # just drop hyphens from `app` value
            pod_role = "".join(pod_app.split("-"))
        pod_labels["app"] = pod_app
        pod_labels["role"] = pod_role
        pod_labels.update(existing_pod_labels)

        existing_selector_labels = glom.glom(
            self.deployment_config, "spec.selector.matchLabels", default={}
        )
        selector_app = glom.glom(existing_selector_labels, "app", default=None)
        selector_role = glom.glom(
            existing_selector_labels, "role", default=None
        )
        if not selector_app:
            selector_labels["app"] = pod_labels["app"]
        if not selector_role:
            selector_labels["role"] = pod_labels["role"]
        selector_labels.update(existing_selector_labels)

        # klio-specific labels
        pod_labels["klio/klio_cli_version"] = klio_cli_version

        # deployment labels
        deploy_user = os.environ.get("USER", "unknown")
        if os.environ.get("CI", "").lower() == "true":
            deploy_user = "ci"
        pod_labels["klio/deployed_by"] = deploy_user

        # any user labels from klio_config.pipeline_options
        # note: if pipeline_options.label (singular) is define in
        # klio-job.yaml, klio-core appends it to pipeline_options.labels
        # (plural) automatically
        user_labels_list = self.klio_config.pipeline_options.labels
        # user labels in beam/klio config are lists of strings, where the
        # strings are key=value pairs, e.g. "keyfoo=valuebar"
        user_labels = {}
        for user_label in user_labels_list:
            if "=" not in user_label:
                # skip - not a valid label; this should technically be
                # caught when validating configuration (not yet implemented)
                continue

            # theoretically user_label could be key=value1=value2, so
            # we just take the first one, but value1=value2 is not
            # valid and will be caught during validation below.
            key, value = user_label.split("=", 1)
            user_labels[key] = value
        pod_labels.update(user_labels)

        path_to_labels = (
            ("metadata.labels", metadata_labels),
            ("spec.selector.matchLabels", selector_labels),
            ("spec.template.metadata.labels", pod_labels),
        )
        for label_path, label_dict in path_to_labels:
            # raises if not valid
            RunPipelineGKE._validate_labels(label_path, label_dict)
            glom.assign(
                self.deployment_config, label_path, label_dict, missing=dict
            )

    def _apply_deployment(self):
        """Create a namespaced deploy if the deployment does not already exist.
        If the namespaced deployment already exists then
        `self.run_job_config.update` will determine if the
        deployment will be updated or not.
        """
        dep = self.deployment_config
        namespace = glom.glom(dep, "metadata.namespace")
        deployment_name = glom.glom(dep, "metadata.name")
        if not self._deployment_exists():
            resp = self.kubernetes_client.create_namespaced_deployment(
                body=dep, namespace=namespace
            )
            deployment_name = resp.metadata.name
            current_cluster = self.kubernetes_active_context["name"]
            logging.info(
                f"Deployment created for {deployment_name} "
                f"in cluster {current_cluster}"
            )
        else:
            if self.run_job_config.update:
                self._update_deployment()
            else:
                logging.warning(
                    f"Cannot apply deployment for {deployment_name}. "
                    "To update an existing deployment, run "
                    "`klio job run --update`, or set `pipeline_options.update`"
                    " to `True` in the job's`klio-job.yaml` file. "
                    "Run `klio job stop` to scale a deployment down to 0. "
                    "Run `klio job delete` to delete a deployment entirely."
                )

    def _setup_docker_image(self):
        super()._setup_docker_image()

        logging.info("Pushing worker image to GCR")
        docker_utils.push_image_to_gcr(
            self._full_image_name,
            self.docker_runtime_config.image_tag,
            self._docker_client,
        )

    def run(self, *args, **kwargs):
        # NOTE: Notice this job doesn't actually run docker locally, but we
        # still have to build and push the image before we can run kubectl

        # docker image setup
        self._check_gcp_credentials_exist()
        self._check_docker_setup()
        self._setup_docker_image()

        self._apply_image_to_deployment_config()
        self._apply_labels_to_deployment_config()

        self._apply_deployment(**kwargs)


class StopPipelineGKE(GKECommandMixin):
    def __init__(self, job_dir):
        super().__init__()
        self.job_dir = job_dir

    def stop(self):
        """Delete a namespaced deployment
        Expects existence of a kubernetes/deployment.yaml
        """
        self._update_deployment(replica_count=0)


class DeletePipelineGKE(GKECommandMixin):
    def __init__(self, job_dir):
        super().__init__()
        self.job_dir = job_dir

    def _delete_deployment(self):
        dep = self.deployment_config
        deployment_name = glom.glom(dep, "metadata.name")
        namespace = glom.glom(dep, "metadata.namespace")
        # Some messaging might change if we multi-cluster deployments
        current_cluster = self.kubernetes_active_context["name"]
        if self._deployment_exists():
            resp = self.kubernetes_client.delete_namespaced_deployment(
                name=deployment_name,
                namespace=namespace,
                body=k8s_client.V1DeleteOptions(
                    propagation_policy="Foreground", grace_period_seconds=5
                ),
            )
            logging.info(f"Deployment deleted: {resp}.")
        else:
            logging.error(
                f"Deployment {namespace}:{deployment_name} "
                f"does not exist in cluster {current_cluster}."
            )

    def delete(self):
        """Delete a namespaced deployment
        Expects existence of a kubernetes/deployment.yaml
        """
        self._delete_deployment()
