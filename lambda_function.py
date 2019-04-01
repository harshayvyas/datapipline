import boto3
import cx_Oracle

def lambda_handler(event,context):
    print("hello world")
    print(cx_Oracle.clientversion())


"""
if __name__=="__main__":
    l=lambda_handler('event','context')
    print(l)
"""