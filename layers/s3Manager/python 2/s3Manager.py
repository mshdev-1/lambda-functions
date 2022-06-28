import boto3
import json


class S3Manager:
    def __init__(self, region_name=None):
        with open("config.json", "r") as conf:
            config = json.load(conf)

        AWS_ACCESS_KEY = config["AWS_ACCESS_KEY"]
        AWS_SECRET_KEY = config["AWS_SECRET_KEY"]
        print("access_key:", AWS_ACCESS_KEY)

        if region_name is None:
            self.s3 = boto3.resource(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY,
                aws_secret_access_key=AWS_SECRET_KEY,
            )
            self.s3_client = boto3.client("s3")
        else:
            self.s3 = boto3.resource(
                "s3",
                region_name=region_name,
                aws_access_key_id=AWS_ACCESS_KEY,
                aws_secret_access_key=AWS_SECRET_KEY,
            )
            self.s3_client = boto3.client("s3")

    def exists(self, bucket_name, key):
        bucket = self.s3.Bucket(bucket_name)
        objects = list(bucket.objects.filter(Prefix=key))

        if objects and objects[0].key == key:
            return True

        return False

    def download_data(self, bucket_name, key, download_path):
        bucket = self.s3.Bucket(bucket_name)
        objects = list(bucket.objects.filter(Prefix=key))

        if objects and objects[0].key == key:
            bucket.download_file(objects[0].key, download_path)
            return True

        return False

    def delete_file(self, bucket_name, key):
        print("delete_file")
        self.s3.meta.client.delete_object(Bucket=bucket_name, Key=key)

    def upload_file(self, bucket_name, key, localpath):
        print("upload_file")
        self.s3.meta.client.upload_file(localpath, bucket_name, key)

    # def get_object(self, bucket_name, key):
    #     bucket = self.s3.Bucket(bucket_name)
    #     objects = list(bucket.objects.filter(Prefix=key))

    #     if objects and objects[0].key == key:
    #         # return objects[0].key
    #         return objects[0]

    #     return None
    def get_object(self, bucket_name, key):
        try:
            # bucket = self.s3.Bucket(bucket_name)
            obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            return obj

        except Exception as e:
            print("[Error] {0}".format(e))
            return None