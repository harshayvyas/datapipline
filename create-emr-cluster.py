import boto3
import time

def lambda_handler(event, context):
    try:
        print("event: {0}".format(event))
        conn = boto3.client("emr")
        # s3 = boto3.resource('s3')

        jobId = run_job(conn)
        print({"cluster_id": str(jobId["JobFlowId"])})
        time.sleep(600.0)
        print("slept 300 secs")
        lambda_response = {
            "output": {
                "cluster_id": jobId["JobFlowId"]
            }
        }

        return lambda_response
    except Exception as e:
        print("exception: event")
        print(e)
        return {'statusCode': 500, 'body': e}


def run_job(conn):
    cluster_id = conn.run_job_flow(
        Name='ondemand-cluster1',
        ServiceRole='CustomerManaged/Custom_DefaultEMRrole',
        JobFlowRole='Custom_EMR_ec2_role',
        VisibleToAllUsers=True,
        LogUri='s3://nexus-emr-input/emr-logs/',
        ReleaseLabel='emr-5.21.0',

        Instances={
            'Ec2SubnetId': 'subnet-0973ffe2a39d62787',
            'EmrManagedMasterSecurityGroup': 'sg-04ffda9d51c8d0a9b',
            'EmrManagedSlaveSecurityGroup': 'sg-064ba73382fe626a4',
            'ServiceAccessSecurityGroup': 'sg-05a2d9304650b7daa',
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.xlarge',
            'InstanceCount': 2,
            'Ec2KeyName': 'nexus-test',
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False
        },
        Applications=[
            {
                'Name': 'Hadoop'
            },
            {
                'Name': 'Hive'
            },
            {
                'Name': 'Spark'
            }
        ],
        Configurations=[{
            "Classification": "spark-env",
            "Properties": {},
            "Configurations": [{
                "Classification": "export",
                "Properties": {
                    "PYSPARK_PYTHON": "/usr/bin/python3"
                }
            }]
        },
            {
                "Classification": "hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                }
            },
            {
                "Classification": "spark",
                "Properties": {
                    "maximizeResourceAllocation": "true"
                }
            }
        ],
        BootstrapActions=[{
            'Name': 'Install',
            'ScriptBootstrapAction': {
                'Path': 's3://nexus-emr-input/bootstrap/install_python_modules.sh'
            }
        }],
        Steps=[],
    )
    return cluster_id
