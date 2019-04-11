import json
import boto3
import time
import urllib


# Returns: cumulative status as COMPLETED / PENDING / FAILED

def lambda_handler(event, context):
    print(event)
    ACTUAL_STATUS_SET = set()
    cluster_id = event["previous-step-output"]["cluster_id"]
    client = boto3.client("emr")
    for step_id in event["previous-step-output"]["step_ids"]:
        response = client.describe_step(
            ClusterId=cluster_id,
            StepId=step_id
        )
        ACTUAL_STATUS_SET.add(response["Step"]["Status"]["State"])

    POSSIBLE_FAILED_STATUS_SET = {"CANCEL_PENDING", "CANCELLED", "FAILED", "INTERRUPTED"}
    POSSIBLE_SUCCESS_STATUS_SET = "COMPLETED"

    ACTUAL_FAILED = POSSIBLE_FAILED_STATUS_SET.intersection(ACTUAL_STATUS_SET)
    print("POSSIBLE_FAILED_STATUS_SET : {0}".format(POSSIBLE_FAILED_STATUS_SET))
    print("ACTUAL_STATUS_SET : {0}".format(ACTUAL_STATUS_SET))
    print("ACTUAL_FAILED : {0}".format(ACTUAL_FAILED))
    if len(ACTUAL_FAILED) != 0:
        return next(iter(ACTUAL_FAILED))
    elif len(ACTUAL_STATUS_SET) == 1 and ACTUAL_STATUS_SET.__contains__(POSSIBLE_SUCCESS_STATUS_SET):
        return POSSIBLE_SUCCESS_STATUS_SET
    else:
        return "PENDING"


