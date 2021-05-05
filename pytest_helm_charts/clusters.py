"""This module introduces classes for handling different clusters."""
import random
import socket
import subprocess
import time
import json

from abc import ABC, abstractmethod
from typing import Optional, Generator
from contextlib import contextmanager, closing

from pykube import HTTPClient, KubeConfig


class Cluster(ABC):
    """Represents an abstract cluster."""

    _kube_client: Optional[HTTPClient]

    def __init__(self):
        self._kube_client = None

    @abstractmethod
    def create(self) -> HTTPClient:
        """Creates an instance of a cluster and returns HTTPClient to connect to it."""
        raise NotImplementedError

    @abstractmethod
    def destroy(self) -> None:
        """Destroys the cluster created earlier with a call to [create](Cluster.create)."""
        raise NotImplementedError

    @property
    def kube_client(self) -> Optional[HTTPClient]:
        """Returns the HTTP client you can use to access Kubernetes API of the cluster under test.

        Please refer to [pykube](https://pykube.readthedocs.io/en/latest/api/pykube.html) to get docs
        for [HTTPClient](https://pykube.readthedocs.io/en/latest/api/pykube.html#pykube.http.HTTPClient).
        """
        return self._kube_client


class ExistingCluster(Cluster):
    """Implementation of [Cluster](Cluster) that uses kube.config
    for an existing cluster.
    """

    kube_config_path: str

    def __init__(self, kube_config_path: str) -> None:
        super().__init__()
        self.kube_config_path = kube_config_path
        self.kubectl_path = "/usr/local/bin/kubectl"

    def create(self) -> HTTPClient:
        kube_config = KubeConfig.from_file(self.kube_config_path)
        self._kube_client = HTTPClient(kube_config)
        return self._kube_client

    def destroy(self) -> None:
        if self._kube_client is None:
            return
        self._kube_client.session.close()
        self._kube_client = None

    # adopted from https://codeberg.org/hjacobs/pytest-kind/src/branch/main/pytest_kind/cluster.py#L130
    # FIXME first argument -> command. if string, then split(" ")
    def exec_kubectl(self, *args: str, **kwargs) -> str:
        """Run a kubectl command against the cluster and return the output as string"""
        # FIXME if kubeconfig_path ..
        return subprocess.check_output(
            [str(self.kubectl_path), *args],
            # env={"KUBECONFIG": str(self.kubeconfig_path)},
            encoding="utf-8",
            **kwargs,
        )

    # def kubectl(self, namespace="default", output="json", *args: str, **kwargs) -> str:
    def kubectl(self, arg_string: str, namespace=None, output="json", input="", **kwargs):
        """Run a kubectl command against the cluster and return the output as string"""
        # FIXME documentation:
        # Idea is to resemble commanline experience and still behave pythonic.
        # Some magic included:
        #
        # FIXME first arg -> command
        # FIXME if possible call kubectl_plain in the end
        # FIXME if input is object run yaml.safe_dump()

        # command = str(self.kubectl_path) or "kubectl"
        # FIXME command here -> kubectl_path or kubectl_binary
        command = self.kubectl_path or "kubectl"

        # FIXME if input then filename="-"?

        # FIXME use options = [] here. copy over kwargs and turn to options-string-list in subprocess
        # make use of `order`? or split between global and sub-command options?
        # FIXME maybe check for order
        # maybe just place some explicit options in order, like --namespace

        # args = []
        # add kubectl_binary
        # namespace if not None
        # arg_string? or array?
        # then options

        if namespace:
            kwargs["namespace"] = namespace
        if output:
            kwargs["output"] = output
        if self.kubeconfig_path:
            kwargs["kubeconfig"] = self.kubeconfig_path
        if input:
            kwargs["filename"] = "-"

        options = {f"--{option}={value}" for option, value in kwargs.items()}

        result = subprocess.check_output([
                command,
                *arg_string.split(" "),
                *options
            ],
            encoding="utf-8",
            input=input
        )

        # return result from kubectl command
        # as parsed json, if requested
        # as list if result is a list
        # as plain text otherwise

        if output == "json":
            output_json = json.loads(result)
            if "items" in output_json:
                return output_json["items"]
            return json.loads(result)
        return result


    # # @contextmanager
    # def port_forward(self, *args, **kwargs) -> Generator[int, None, None]:
    #     """Run "kubectl port-forward" for the given service/pod and use a random local port"""
    #     return KindClusterUpstream.port_forward(self, *args, **kwargs)

    # adopted from https://codeberg.org/hjacobs/pytest-kind/src/branch/main/pytest_kind/cluster.py#L141
    @contextmanager
    def port_forward(
        self,
        service_or_pod_name: str,
        remote_port: int,
        # *args,
        local_port: int = None,
        retries: int = 10,
        **kwargs
    ) -> Generator[int, None, None]:
        """Run "kubectl port-forward" for the given service/pod and use a random local port"""
        port_to_use: int
        proc = None

        command = self.kubectl_path or "kubectl"

        # if namespace:
        #     kwargs["namespace"] = namespace
        # if output:
        #     kwargs["output"] = output
        if self.kubeconfig_path:
            kwargs["kubeconfig"] = self.kubeconfig_path

        options = {f"--{option}={value}" for option, value in kwargs.items()}


        for i in range(retries):
            if proc:
                proc.kill()
            # Linux epheremal port range starts at 32k
            port_to_use = local_port or random.randrange(5000, 30000)
            proc = subprocess.Popen(
                [
                    command,
                    *options,
                    "port-forward",
                    service_or_pod_name,
                    f"{port_to_use}:{remote_port}",
                    # *args,
                ],
                # env={"KUBECONFIG": str(self.kubeconfig_path)},
                # FIXME use https://docs.python.org/3/library/subprocess.html#subprocess.run instead?
                # and capture output
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time.sleep(1)
            returncode = proc.poll()
            if returncode is not None:
                if i >= retries - 1:
                    raise Exception(
                        f"kubectl port-forward returned exit code {returncode}"
                    )
                else:
                    # try again
                    continue

            # https://docs.python.org/3/library/contextlib.html#contextlib.closing

            with closing(socket.socket()) as s:
                try:
                    s.connect(("127.0.0.1", port_to_use))
                except:
                    if i >= retries - 1:
                        raise

        try:
            yield port_to_use
        finally:
            if proc:
                proc.kill()
