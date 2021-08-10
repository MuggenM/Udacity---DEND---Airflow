from time import time
from time import sleep
import sys
import boto3
import configparser
import json


def delete_cluster(cfile):
    """
    - Removes a RedShift cluster from AWS, also removes role and database

    Parameters:
        - cfile (File path): The filepath to th configuration file
    """

    # Use configparser to read in the variables the aws config file dw.cfg
    config = configparser.ConfigParser()
    config.read_file(open(cfile))
    DWH_IAM_ROLE_NAME = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")
    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
    REGION = config.get('AWS','region')
    KEY = config.get('AWS','key')
    SECRET = config.get('AWS','secret')

    redshift = boto3.client('redshift',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )

    iam = boto3.client('iam',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )

    ec2 = boto3.resource('ec2',
                        region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    #### CAREFUL!!
    # delete the created resources
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)



    # Run while loop until sever is not available anymore
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    status_shift = myClusterProps.get("ClusterStatus")

    print("deleting cluster...",)
    syms = ['\\', '|', '/', '-']
    bs = '\b'
    while status_shift == 'deleting':
        try:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            status_shift = myClusterProps.get("ClusterStatus")
            for sym in syms:
                sys.stdout.write("\b%s" % sym)
                sys.stdout.flush()
                sleep(.1)
        except Exception as e:
            print("Clusterd deleted")
            status_shift = "error"


    # delete the created resources
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)



def main():
    """
    - Delete a RedShift cluster on AWS.

    """

    config = configparser.ConfigParser()
    config.read('cluster.cfg')

    delete_cluster('cluster.cfg')




if __name__ == "__main__":
    main()