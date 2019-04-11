import json
import boto3


def lambda_handler(event, context):
    # TODO implement
    print("event : {0}".format(event))
    s3 = boto3.resource('s3')
    conn = boto3.client('emr')
    content_object = s3.Object("nexus-emr", "conf/nexus_jobs.json")
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)

    steps = []
    for step in event['params']:
        print(json_content[step])
        steps.append(json_content[step])
    print(steps)
    print(event["previous-step-output"]["cluster_id"])
    response = run_step(conn, event["previous-step-output"]["cluster_id"], steps)
    print(response)
    return {
        "output": {
            "cluster_id": event["previous-step-output"]["cluster_id"],
            "step_ids": response["StepIds"]
        }
    }


def run_step(conn, cluster_id, steps):
    action = conn.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)
    return action