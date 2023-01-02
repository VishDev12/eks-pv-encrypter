"""Functions that interact directly with the Kubernetes Client.
"""

import traceback
from kubernetes import client, config
from typing import Dict, List, Union, Any
from kubernetes.client.models.v1_pod import V1Pod
from kubernetes.client.models.v1_pod_list import V1PodList
from kubernetes.client.models.v1_persistent_volume import V1PersistentVolume
from kubernetes.client.models.v1_persistent_volume_list import V1PersistentVolumeList
from kubernetes.client.models.v1_persistent_volume_claim import V1PersistentVolumeClaim

from eks_pv_encrypter.logger import logger

config.load_kube_config()

v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()


def get_pv_list() -> V1PersistentVolumeList:
    """Return a list of all persistent volumes in the K8s cluster.

    Returns
    -------
    V1PersistentVolumeList
        List of PVs in the cluster.
    """
    pv_list = v1.list_persistent_volume()

    logger.info(f"Count of Persistent Volumes (PVs): {len(pv_list.items)}")

    return pv_list


def get_pod_list() -> V1PodList:
    """Get a list of all (only) running pods in the K8s cluster.

    Returns
    -------
    V1PodList
        List of all pods in the cluster.
    """
    pod_list = v1.list_pod_for_all_namespaces(field_selector="status.phase=Running")

    logger.info(f"Count of Pods: {len(pod_list.items)}")

    return pod_list


def get_ebs_backed_pvs(pv_list: V1PersistentVolumeList) -> List[V1PersistentVolume]:
    """Given a list of PVs, return the subset of PVs that are backed by an EBS volume.

    We ignore PVs that have EBS volumes provisioned by the EBS CSI driver. In my case,
    it's because the default driver is configured to launch encrypted volumes.

    Parameters
    ----------
    pv_list : V1PersistentVolumeList
        List of Persistent Volumes.

    Returns
    -------
    list of V1PersistentVolume
        Subset of input PV list with PVs backed by EBS volumes.
    """
    ebs_pv_list = []

    if not pv_list.items:
        raise ValueError("No PVs passed to the 'get_ebs_backed_pvs' function.")

    for pv in pv_list.items:

        pv: V1PersistentVolume
        assert pv.spec

        if pv.spec.aws_elastic_block_store:
            ebs_pv_list.append(pv)

    logger.info(f"Count of PVs backed by EBS volumes: {len(ebs_pv_list)}")

    return ebs_pv_list


def get_owner_details(pod: V1Pod) -> Dict[str, str]:
    """Given a pod, find the name of the owner, either a Statefulset or Deployment.

    NOTES
    -----
    1. I hear there are rare cases where an object can have multiple owners; this code
    does not account for those cases.
    2. I've only considered pods that have Statefulsets and Deployments as their owners.
    There could be other qualifying pods launched by other resource types, but since
    there's no unified way to scale them down, we record them, but no action will be
    taken.

    Cases
    -----
    StatefulSet -> Pod
    Deployment -> ReplicaSet -> Pod

    Parameters
    ----------
    pod : V1Pod
        A single pod definition.

    Returns
    -------
    Dict[str, Any]
        Dictionary with the kind and name of the owners of the pod.
    """

    # If the immediate owner of the pod is a ReplicaSet, return the owner of the
    # ReplicaSet. This covers Deployments.
    if pod.metadata.owner_references[0].kind == "ReplicaSet":
        # Check if the ReplicaSet belongs to a Deployment.
        replicaset = apps_v1.read_namespaced_replica_set(
            name=pod.metadata.owner_references[0].name,
            namespace=pod.metadata.namespace,
        )

        return {
            "kind": replicaset.metadata.owner_references[0].kind,
            "name": replicaset.metadata.owner_references[0].name,
        }

    # Else return the owner of the pod directly. This covers StatefulSets.
    return {
        "kind": pod.metadata.owner_references[0].kind,
        "name": pod.metadata.owner_references[0].name,
    }


def scale_deployment(name: str, namespace: str, replicas: int) -> Dict[str, Any]:
    """Given the name of a deployment, scale it to the specified number of replicas.

    Parameters
    ----------
    name : string
        The name of the Deployment.
    namespace : string
        The namespace of the Deployment.
    replicas : int
        The number of replicas to which the deployment should be scaled.

    Returns
    -------
    Dict[str, Any]
        Response after the patching operation.
    """
    return apps_v1.patch_namespaced_deployment_scale(
        name=name,
        namespace=namespace,
        body={
            "spec": {"replicas": replicas},
        },
    )


def scale_stateful_set(name: str, namespace: str, replicas: int) -> Dict[str, Any]:
    """Given the name of a StatefulSet, scale it to the specified number of replicas.

    Parameters
    ----------
    name : string
        The name of the StatefulSet.
    namespace : string
        The namespace of the StatefulSet.
    replicas : int
        The number of replicas to which the StatefulSet should be scaled.

    Response
    --------
    Dict[str, Any]
        The response of the request to `patch_namespaced_stateful_set_scale`.
    """
    return apps_v1.patch_namespaced_stateful_set_scale(
        name=name,
        namespace=namespace,
        body={
            "spec": {
                "replicas": replicas,
            },
        },
    )


def clean_pv_pvc(obj: Union[V1PersistentVolume, V1PersistentVolumeClaim], type: str):
    """Delete unnecessary fields from the PV manifest.

    Parameters
    ----------
    obj : V1PersistentVolume or V1PersistentVolumeClaim
        The PV/PVC object that needs to be stripped of unnecessary fields.
    type : string
        The object type. Either "pv" or "pvc".

    Returns
    -------
    V1PersistentVolume or V1PersistentVolumeClaim
        The cleaned object that was input.
    """

    obj.metadata.annotations = None
    obj.metadata.creation_timestamp = None
    obj.metadata.resource_version = None
    obj.metadata.uid = None
    obj.metadata.managed_fields = None
    obj.metadata.self_link = None

    obj.status = None

    if type == "pv":
        obj.spec.claim_ref.uid = None
        obj.spec.claim_ref.resource_version = None

    return obj


def get_pvc(claim_ref_name: str, claim_ref_namespace: str) -> Any:
    """Given the name and namespace of the claim reference of a PV, return the PVC
    object.

    Parameters
    ----------
    claim_ref_name : string
        The name of the claim reference.
    claim_ref_namespace : string
        The namespace of the claim reference.

    Returns
    -------
    V1PersistentVolumeClaim
        The PVC object.
    """

    # Find the PVC linked to the PV.
    return v1.read_namespaced_persistent_volume_claim(
        name=claim_ref_name,
        namespace=claim_ref_namespace,
    )


def pv_exists(name: str, fail_fast: bool = False):
    """Check for the existence of a PV given a name.

    Parameters
    ----------
    name : str
        The name of the PV.
    fail_fast : bool
        If True, throw an exception if the PV doesn't exist or has an issue.

    Returns
    -------
    boolean
        True if the PV exists else False.
    """
    try:
        pv_response = v1.read_persistent_volume(name=name)

        return True

    except Exception as e:
        logger.error(f"Error when checking on the status of PVC: {name}")
        logger.error(traceback.format_exc())

        if fail_fast:
            # Exit.
            raise e
        else:
            return False


def pvc_exists(name: str, namespace: str, fail_fast: bool = False):
    """Check for the existence of a PVC given a name.

    Parameters
    ----------
    name : str
        The name of the PVC.
    namespace : str
        The namespace of the PVC.
    fail_fast : bool
        If True, throw an exception if the PVC doesn't exist or has an issue.

    Returns
    -------
    boolean
        True if the PVC exists else False.
    """
    try:
        pvc_response = v1.read_namespaced_persistent_volume_claim(
            name=name, namespace=namespace
        )

        return True

    except Exception as e:
        logger.error(f"Error when checking on the status of PVC: {name} in {namespace}")
        logger.error(traceback.format_exc())

        if fail_fast:
            # Exit.
            raise e
        else:
            return False
