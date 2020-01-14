import boto3
import json
from json_schema import json_schema
from itertools import groupby
from collections import OrderedDict


class BucketScanner:
    """A class scanning the s3 bucekt for data feeds from stac"""

    def __init__(self, profile_name: str, bucket_name: str, prefix: str):
        """ constructor for calss BuckerScanner """
        self.profile_name = profile_name
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.session = boto3.session.Session(profile_name=self.profile_name)
        self.s3_client = self.session.client("s3")

    def scan(self, stream: str, partition: str) -> (int, list):
        """ A method to scan objects for a given stream """
        pre = "{}/{}/{}/".format(self.prefix, stream, partition)
        response = self.s3_client.list_objects(
            Bucket=self.bucket_name,
            Prefix=pre
        )
        return len(response["Contents"]), response["Contents"]

    def extract_schema(self, metadata: list):
        """ A method to extract schema from the stream feeds """
        schemas: list(str) = []
        for item in metadata:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=item["Key"]
            )
            feed_str = response["Body"].read().decode("utf-8")
            # Hack for extra data found in the feeds
            feed_str = feed_str.split("}{", 1)[0]
            feed_str = feed_str + "}" if not feed_str.endswith("}") else feed_str
            print(feed_str)
            schemas.append(json_schema.dumps(feed_str))

        schema_occurence = {key: schemas.count(key) for key in schemas}
        schema_set: set(str) = set(schemas)
        distinct_schema_count = len(schema_set)
        print(f"No. of distinct schemas {distinct_schema_count}")
        print("======== schema occurences ========")
        for k, v in schema_occurence.items():
            print(f"{k} <:> {v}")
        print("\n\n")
