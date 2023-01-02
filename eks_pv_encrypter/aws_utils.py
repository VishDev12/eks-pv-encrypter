"""Functions that interact directly with the boto3 EC2 client.
"""

import os
import boto3
import traceback
import botocore.exceptions
from typing import Dict, List, Any, Union

from eks_pv_encrypter.logger import logger

# A client to interact with EC2 resources in the AWS region specified by the AWs_REGION
# environment variable.
ec2_client = boto3.client("ec2", region_name=os.environ["AWS_REGION"])


def get_ebs_details(volume_id: str) -> Dict[str, Any]:
    """Given an EBS volume ID, return its details.

    Parameters
    ----------
    volume_id : string
        EBS Volume ID starting with `vol-`.

    Returns
    -------
    dict[str, Any]
        The response of the describe volumes request. (First element of the value of
        the "Volumes" key.)
    """

    try:
        return ec2_client.describe_volumes(VolumeIds=[volume_id])["Volumes"][0]

    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InvalidVolume.NotFound":
            logger.error(f"The volume {volume_id} was not found in AWS.")

        raise e


def get_snapshot_details(snapshot_id: str) -> Dict[str, Any]:
    """Given a snapshot ID, check whether the corresponding snapshot is complete."""

    return ec2_client.describe_snapshots(SnapshotIds=[snapshot_id])["Snapshots"][0]


def create_ebs_snapshot(
    volume_id: str, extra_log_info: str, tags: List[Dict[str, str]]
) -> Union[bool, str]:
    """Given the ID of an EBS volume, create a snapshot.

    This snapshot will be unencrypted if the volume is also unencrypted.

    Parameters
    ----------
    volume_id : string
        EBS Volume ID starting with `vol-`.
    extra_log_info : string
        A string with additional information that can be added to the description of
        the EBS Snapshot that will be created.
    tags : list of dict[str, str]
        A set of tags that will be applied to the Snapshot that will be created.

    Returns
    -------
    False or str
        Returns False if an error was thrown.
        Returns the Snapshot ID if creation was successful.
    """

    try:
        tags += [
            {
                "Key": "created_by",
                "Value": "pv_encrypter",
            },
            {
                "Key": "source_function",
                "Value": "create_ebs_snapshot",
            },
            {
                "Key": "volume",
                "Value": volume_id,
            },
        ]
        response = ec2_client.create_snapshot(
            Description=f"A snapshot of volume: {volume_id}.\n\nAdditional information: {extra_log_info}",
            VolumeId=volume_id,
            TagSpecifications=[{"ResourceType": "snapshot", "Tags": tags}],
        )

        return response["SnapshotId"]

    except Exception as e:
        logger.error(f"Snapshot creation failed. Volume ID: {volume_id}")
        logger.error(traceback.format_exc())

        return False


def encrypt_ebs_snapshot(
    snapshot_id: str, extra_log_info: str, tags: List[Dict[str, str]]
) -> Union[bool, str]:
    """Given a snapshot ID, create an encrypted copy in the same region.

    Parameters
    ----------
    snapshot_id : string
        EBS Snapshot ID starting with `snap-`.
    extra_log_info : string
        A string with additional information that can be added to the description of
        the encrypted EBS Snapshot that will be created.
    tags : list of dict[str, str]
        A set of tags that will be applied to the Snapshot that will be created.

    Returns
    -------
    False or str
        Returns False if an error was thrown.
        Returns the Snapshot ID if creation was successful.
    """

    try:
        tags += [
            {
                "Key": "created_by",
                "Value": "pv_encrypter",
            },
            {
                "Key": "source_function",
                "Value": "encrypt_ebs_snapshot",
            },
            {
                "Key": "snapshot",
                "Value": snapshot_id,
            },
        ]

        response = ec2_client.copy_snapshot(
            Description=f"A copy of snapshot: {snapshot_id}.\n\nAdditional information: {extra_log_info}",
            Encrypted=True,
            SourceRegion=os.environ["AWS_REGION"],
            SourceSnapshotId=snapshot_id,
            TagSpecifications=[{"ResourceType": "snapshot", "Tags": tags}],
        )

        return response["SnapshotId"]

    except Exception as e:
        logger.error(f"Snapshot copy failed. Snapshot ID: {snapshot_id}")
        logger.error(traceback.format_exc())

        return False


def create_ebs_volume_from_snapshot(
    snapshot_id: str, availability_zone: str, tags: List[Dict[str, str]]
):
    """Given a snapshot ID, create an encrypted EBS volume.

    NOTE: The `Encrypted=True` below isn't strictly necessary because we create the volume
    from an Encrypted Snapshot which automatically creates an encrypted Volume.

    Parameters
    ----------
    snapshot__id : string
        EBS Snapshot ID starting with `snap-`.
    availability_zone : str
        The AWS AZ in which the EBS Volume will be created. Ex.: `us-east-1`.
    tags : list of dict[str, str]
        A set of tags that will be applied to the Volume that will be created.

    Returns
    -------
    False or str
        Returns False if an error was thrown.
        Returns the Volume ID if creation was successful.
    """

    try:
        tags += [
            {
                "Key": "created_by",
                "Value": "pv_encrypter",
            },
            {
                "Key": "source_function",
                "Value": "encrypt_ebs_snapshot",
            },
            {
                "Key": "snapshot",
                "Value": snapshot_id,
            },
        ]

        # Create volume from encrypted snapshot.
        response = ec2_client.create_volume(
            SnapshotId=snapshot_id,
            AvailabilityZone=availability_zone,
            Encrypted=True,
            VolumeType="gp3",  # NOTE: Assumption made.
            TagSpecifications=[{"ResourceType": "volume", "Tags": tags}],
        )

        return response["VolumeId"]

    except Exception as e:
        logger.error(
            f"Volume creation failed. Snapshot ID: {snapshot_id}, AZ: {availability_zone}"
        )
        logger.error(traceback.format_exc())

        return False


def volume_exists(volume_id: str, fail_fast: bool = False) -> bool:
    """Given a volume ID, check whether the volume exists in EBS.

    Parameters
    ----------
    volume_id : string
        EBS Volume ID starting with `vol-`.
    fail_fast : boolean
        True -> Error throws exception.
        False -> Error returns False.

    Returns
    -------
    boolean
        True if the Volume exists else False.
    """
    try:
        _ = ec2_client.describe_volumes(VolumeIds=[volume_id])["Volumes"][0]

        return True

    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "InvalidVolume.NotFound":
            logger.error(f"The volume {volume_id} was not found in AWS.")

        logger.error(f"Error when checking on the status of Volume: {volume_id}")
        logger.error(traceback.format_exc())

        if fail_fast:
            # Exit.
            raise e
        else:
            return False
