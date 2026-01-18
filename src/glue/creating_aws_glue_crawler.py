import boto3
import logging
import os
from botocore.exceptions import ClientError

#Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

my_crawler_name = 'crypto_glue_crawler'
my_iam_role = os.getenv('AWS_GLUE_IAM_ROLE')
my_glue_db_name = 'crypto_db'
my_prefix = 'raw_'
s3_bucket = os.getenv('S3_BUCKET', 'julian-crypto-s3-bucket')
my_s3_target = f's3://{s3_bucket}/'

if not my_iam_role:
    logger.error("AWS_GLUE_IAM_ROLE environment variable is required but not set")
    raise ValueError("AWS_GLUE_IAM_ROLE environment variable must be set")

class GlueWrapper:
    """Encapsulates AWS Glue actions."""

    def __init__(self, glue_client):
        """
        :param glue_client: A Boto3 Glue client.
        """
        self.glue_client = glue_client


    def create_crawler(self, name, role_arn, db_name, db_prefix, s3_target):
        """
        Creates a crawler that can crawl the specified target and populate a
        database in your AWS Glue Data Catalog with metadata that describes the data
        in the target.

        :param name: The name of the crawler.
        :param role_arn: The Amazon Resource Name (ARN) of an AWS Identity and Access
                         Management (IAM) role that grants permission to let AWS Glue
                         access the resources it needs.
        :param db_name: The name to give the database that is created by the crawler.
        :param db_prefix: The prefix to give any database tables that are created by
                          the crawler.
        :param s3_target: The URL to an S3 bucket that contains data that is
                          the target of the crawler.
        """
        try:
            self.glue_client.create_crawler(
                Name=name,
                Role=role_arn,
                DatabaseName=db_name,
                TablePrefix=db_prefix,
                Targets={"S3Targets": [{"Path": s3_target}]},
            )
        except ClientError as err:
            logger.error(
                "Couldn't create crawler. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

if __name__ == "__main__":
    glue_client = boto3.client('glue')
    glue_wrapper = GlueWrapper(glue_client)
    glue_wrapper.create_crawler(my_crawler_name, my_iam_role, my_glue_db_name, my_prefix, my_s3_target)
