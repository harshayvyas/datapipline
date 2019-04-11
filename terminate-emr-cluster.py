import json
import boto3

# Sample Input : {
#
#     "cluster_id": "j-OFQFN5GNWXV9"
#   }
# }
def lambda_handler(event, context):
    conn = boto3.client('emr')
    return conn.terminate_job_flows(JobFlowIds=[event["previous-step-output"]["cluster_id"]])
