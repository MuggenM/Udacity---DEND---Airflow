from time import time
from time import sleep
import sys
import boto3
import configparser
import json


def create_cluster(cfile):
    """
    - Creates a RedShift cluster on AWS and a database
    - DWH_ROLE_ARN, DWH_ENDPOINT are written back to config file

    Parameters:
        - cfile (File path): The filepath to th configuration file
    """

    # Use configparser to read in the variables the aws config file dw.cfg
    config = configparser.ConfigParser()
    config.read_file(open(cfile))
    KEY = config.get('AWS','key')
    SECRET = config.get('AWS','secret')
    REGION = config.get('AWS','region')

    DWH_DB = config.get("DWH","DWH_DB")
    DWH_DB_USER = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH","DWH_PORT")


    DWH_IAM_ROLE_NAME = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")
    DWH_CLUSTER_TYPE = config.get("CLUSTER","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("CLUSTER","DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("CLUSTER","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")


    # Create clients for EC2, S3, IAM, and Redshift
    ec2 = boto3.resource('ec2',
                        region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    s3 = boto3.resource('s3',
                        region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    iam = boto3.client('iam',
                        region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    redshift = boto3.client('redshift',
                            region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )



    # Create an IAM role and assign it a policy so that it can read S3 bucket
    #1.1 Create the role,
    try:
        print("1.1 Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                  'Effect': 'Allow',
                  'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    # Attach a policy to a role
    print("1.2 Attaching Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                          PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']


    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    # Create a Redshift Cluster and Launch it
    print("1.4 Create a Redshift Cluster")
    # Create a Redshift Cluster and Launch it
    try:
        response = redshift.create_cluster(
            # add parameters for hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # add parameters for identifiers & credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # add role (to allow s3 access)
            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)


    # Run while loop until sever status is available
    print("1.5 Wait for cluster status is available")
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    status_shift = myClusterProps.get("ClusterStatus")

    print("waiting for cluster...",)
    syms = ['\\', '|', '/', '-']
    bs = '\b'
    while status_shift == 'creating':
        try:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            status_shift = myClusterProps.get("ClusterStatus")
            for sym in syms:
                sys.stdout.write("\b%s" % sym)
                sys.stdout.flush()
                sleep(.1)
        except Exception as e:
            status_shift = 'Error'
            print(e)


    # Create endpoint
    print("1.6 Create endpoint")
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)


    # Open an incoming  TCP port to access the cluster ednpoint
    print("1.7 Create inbound rule for server")
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName= 'default',
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

    # update DWH_ROLE_ARN and DWH_ENDPOINT entries the config file
    config.set('CLUSTER', 'DWH_ROLE_ARN', DWH_ROLE_ARN)
    config.set('DWH', 'DWH_ENDPOINT', DWH_ENDPOINT)
    with open(cfile, 'w') as configfile:
        config.write(configfile)



def main():
    """
    - Create a RedShift cluster on AWS.

    """

    config = configparser.ConfigParser()
    config.read('cluster.cfg')

    create_cluster('cluster.cfg')


if __name__ == "__main__":
    main()