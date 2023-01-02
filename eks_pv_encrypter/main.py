"""Common functions.
"""

import traceback
import contextlib
from rich.table import Table
from rich.console import Console
from typing import List, Dict, Any, Literal
from kubernetes.client.models.v1_pod import V1Pod
from kubernetes.client.models.v1_pod_list import V1PodList
from kubernetes.client.models.v1_persistent_volume import V1PersistentVolume

from eks_pv_encrypter.logger import logger
from eks_pv_encrypter.aws_utils import get_ebs_details, get_snapshot_details
from eks_pv_encrypter.k8s_utils import (
    get_owner_details,
    get_pv_list,
    get_pod_list,
    get_ebs_backed_pvs,
)

console = Console()


def get_volume_id_short(volume_id_long: str):
    """The volume_id stored in the pv begins with `aws://<region>/` which is not used in
    the ec2 client when describing volumes. Return a version without that prefix.

    Parameters
    ----------
    volume_id_long : string
        The long Volume ID that's recorded in the Persistent Volume.

    Returns
    -------
    string
        The Volume ID without the prefix `aws://<region>/`.
    """
    return volume_id_long[volume_id_long.index("vol-") :]


def get_unencrypted_ebs_pvs(
    ebs_pv_list: List[V1PersistentVolume],
) -> List[V1PersistentVolume]:
    """Given a list of EBS backed PVs, return the subset with EBS volumes that are
    unencrypted.

    Parameters
    ----------
    ebs_pv_list : list of V1PersistentVolume
        List of Persistent Volumes backed by EBS Volumes.

    Returns
    -------
    list of V1PersistentVolume
        Subset of input list with unencryted EBS Volumes.
    """

    enc_count = 0  # A count of encrypted EBS volumes in the cluster.
    unenc_pv_list = []  # A list of unencrypted EBS volumes in the cluster.

    # For every PV in the list.
    for pv in ebs_pv_list:
        try:
            # Retrieve the details of the linked volume.
            volume_id = get_volume_id_short(pv.spec.aws_elastic_block_store.volume_id)
            volume_details = get_ebs_details(volume_id=volume_id)

            # Check whether it's encrypted.
            if volume_details["Encrypted"]:
                enc_count += 1
            else:
                unenc_pv_list.append(pv)

        except Exception as e:

            # In case volume_id is not present.
            with contextlib.suppress(Exception):
                logger.error(f"Error with volume: {volume_id}")

            logger.error(f"Affected PV: {pv.metadata.name}")
            logger.error(
                f"Affected PVC: {pv.spec.claim_ref.name} in {pv.spec.claim_ref.namespace}"
            )
            logger.error(e)

    logger.info(f"Count of encrypted EBS PVs: {enc_count}")
    logger.info(f"Count of unencrypted EBS PVs: {len(unenc_pv_list)}")

    return unenc_pv_list


def filter_pods_with_pv(
    pod_list: V1PodList, pv_list: List[V1PersistentVolume]
) -> List[Dict[str, Any]]:
    """Given a list of pods, return the subset that uses persistent volume claims that
    are linked to a PV from the pv_list.

    NOTE
    ----
    If there are no such pods, the implication is that the corresponding
    Deployment or StatefulSet has already been scaled down.

    Parameters
    ----------
    pod_list : V1PodList
        List of pods.
    pv_list : list of V1PersistentVolume
        List of PVs used to filter out pods from the pod_list.

    Returns
    -------
    list of dict[str, Any]
        Subset of input pod_list linked to one of the PVs in pv_list.
    """
    object_list = []

    if not pod_list.items:
        raise ValueError(
            "No Pods in the list passed to the 'get_pods_with_pv' function."
        )

    # Construct a list of PVC names from the list of Persistent Volumes.
    pvc_name_list = [
        f"{pv.spec.claim_ref.name}|{pv.spec.claim_ref.namespace}" for pv in pv_list
    ]

    for pod in pod_list.items:

        pod: V1Pod
        assert pod.spec

        if not pod.spec.volumes:
            continue

        for volume in pod.spec.volumes:
            # Value is None if not present.
            if volume.persistent_volume_claim:

                # Name of the PVC linked to the volume.
                claim_name = volume.persistent_volume_claim.claim_name
                claim_namespace = pod.metadata.namespace

                # Filter only if pvc_name_list is present.
                if f"{claim_name}|{claim_namespace}" in pvc_name_list:

                    # Find PV corresponding to claim_name.
                    pv_list_index = pvc_name_list.index(
                        f"{claim_name}|{claim_namespace}"
                    )
                    pv = pv_list[pv_list_index]

                    # Link pv to pod in the dictionary below.
                    object_list.append(
                        {
                            "pod": pod,
                            "pv": pv,
                            "pvc_name": claim_name,
                            "pv_list_index": pv_list_index,
                        }
                    )

    logger.info(f"Count of Pods with unencrypted PVCs attached: {len(object_list)}")

    return object_list


def get_owners(
    pod_list: V1PodList,
    unenc_pv_list: List[V1PersistentVolume],
):
    """Given a list of pods and a list of PVs, find the pods linked to those PVs, then
    return the true owners of the pods, i.e: Deployment, StatefulSet, etc.

    Parameters
    ----------
    pod_list : V1PodList
        List of pods.
    unenc_pv_list : list of V1PersistentVolume
        List of PVs used to filter out pods from the pod_list.

    Returns
    -------
    list of dict[str, Any]
        Subset of input pod_list linked to one of the PVs in pv_list.
    """

    # Get the subset of pods whose PVCs are unencrypted and backed by EBS.
    object_list = filter_pods_with_pv(pod_list, unenc_pv_list)

    # Construct a dictionary of deployments and statefulsets that control these pods.
    # Since one deployment/statefulset can have many pods, we have a nested dictionary
    # ending with an innermost list to track these relationships.
    # Deployment -> name -> pods
    # StatefulSet -> name -> pods
    # {str: {str: [V1Pod]}}

    owner_dict: Dict[
        Literal["Deployment", "StatefulSet"], Dict[str, List[Dict[str, Any]]]
    ] = {
        "Deployment": {},
        "StatefulSet": {},
    }

    for obj_dict in object_list:

        # Get the details of the pod's owner.
        # This returns "kind" and "name" in a dictionary.
        owner_details = get_owner_details(obj_dict["pod"])

        kind = owner_details["kind"]

        # Treat Deployments and StatefulSets separately, since we know them.
        if kind in ["Deployment", "StatefulSet"]:

            # We need to track the name of the deployment/statefulset along with the
            # namespace.
            name = obj_dict["pod"].metadata.namespace + "|" + owner_details["name"]

            # This list is used to track the pods and pv under each deployment or statefulset.
            if name not in owner_dict[kind]:
                owner_dict[kind][name] = []

            owner_dict[kind][name].append(obj_dict)

        else:
            logger.warning(
                f"The owner of pod {obj_dict['pod'].metadata.name} in "
                f"the {obj_dict['pod'].metadata.namespace} namespace "
                f"is a {kind}, which isn't supported. Please ensure you scale this down "
                f"correctly before proceeding. PV Index: {obj_dict['pv_list_index']}"
            )

    logger.info(f"Number of qualifying Deployments: {len(owner_dict['Deployment'])}")
    table = Table(
        title="Qualifying Deployments",
        show_lines=True,
        highlight=True,
        expand=True,
    )
    table.add_column("Index", overflow="fold")
    table.add_column("Namespace", max_width=10, overflow="fold")
    table.add_column("Name", max_width=10, overflow="fold")
    table.add_column("# Pods")
    table.add_column("PVC Names", max_width=12, overflow="fold")
    table.add_column("PV Names", max_width=12, overflow="fold")
    table.add_column("PV Index", max_width=12, overflow="fold")

    for i, (name, obj_dict_ls) in enumerate(owner_dict["Deployment"].items()):
        table.add_row(
            str(i),
            name.split("|")[0],
            name.split("|")[1],
            str(len(obj_dict_ls)),
            str([obj_dict["pvc_name"] for obj_dict in obj_dict_ls]),
            str([obj_dict["pv"].metadata.name for obj_dict in obj_dict_ls]),
            str([obj_dict["pv_list_index"] for obj_dict in obj_dict_ls]),
        )

    table.add_row(
        "Total",
        "---",
        "---",
        str(sum(len(obj_dict_ls) for obj_dict_ls in owner_dict["Deployment"].values())),
        "---",
        "---",
        "---",
    )
    console.print(table)

    logger.info(f"Number of qualifying StatefulSets: {len(owner_dict['StatefulSet'])}")
    table = Table(
        title="Qualifying StatefulSets",
        show_lines=True,
        highlight=True,
        expand=True,
    )
    table.add_column("Index", overflow="fold")
    table.add_column("Namespace", max_width=10, overflow="fold")
    table.add_column("Name", max_width=10, overflow="fold")
    table.add_column("# Pods")
    table.add_column("PVC Names", max_width=12, overflow="fold")
    table.add_column("PV Names", max_width=12, overflow="fold")
    table.add_column("PV Index", max_width=12, overflow="fold")

    for i, (name, obj_dict_ls) in enumerate(owner_dict["StatefulSet"].items()):
        table.add_row(
            str(i),
            name.split("|")[0],
            name.split("|")[1],
            str(len(obj_dict_ls)),
            str([obj_dict["pvc_name"] for obj_dict in obj_dict_ls]),
            str([obj_dict["pv"].metadata.name for obj_dict in obj_dict_ls]),
            str([obj_dict["pv_list_index"] for obj_dict in obj_dict_ls]),
        )

    table.add_row(
        "Total",
        "---",
        "---",
        str(
            sum(len(obj_dict_ls) for obj_dict_ls in owner_dict["StatefulSet"].values())
        ),
    )
    console.print(table)

    return owner_dict


def get_snapshot_list_progress(snapshot_id_ls: List[str]) -> Dict[str, Any]:
    """Given a list of snapshot IDs, check whether they've all been completed.

    Return the average progress of all snapshots in the list.

    Parameters
    ----------
    snapshot_id_ls : list of strings
        List of Snapshot IDs.

    Returns
    -------
    Dict[str, Any]
        Dictionary of progress_list and state_list which has the progress and state for
        each snapshot; avg_progress which has the mean progress for all snapshots.
    """

    progress_list = []
    state_list = []
    for snapshot_id in snapshot_id_ls:
        snapshot_details = get_snapshot_details(snapshot_id)

        progress_list.append(int(snapshot_details["Progress"].strip("%")))
        state_list.append(snapshot_details["State"])

    return {
        "progress_list": progress_list,
        "state_list": state_list,
        "avg_progress": sum(progress_list) / len(progress_list),
    }


def collect_info():
    # Get the list of all Persistent Volumes in the Cluster.
    pv_list = get_pv_list()

    # Get all PVs that are backed by EBS Volumes.
    ebs_pv_list = get_ebs_backed_pvs(pv_list)

    # Find the PVs with unencrypted EBS Volumes.
    # NOTE: This is the main list we use for the rest of the notebook.
    # So the order of the `unenc_pv_list` will be maintained and used in enc_snapshot_id_ls
    # and volume_id_ls.
    unenc_pv_list = get_unencrypted_ebs_pvs(ebs_pv_list)

    # Get the list of all Pods.
    pod_list = get_pod_list()

    # Get all the owners of the pods that are linked to one of the PVs from `unenc_pv_list`
    # through a PVC.
    valid_owners = get_owners(pod_list, unenc_pv_list)
