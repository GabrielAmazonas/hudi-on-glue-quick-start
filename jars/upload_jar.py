import boto3
import sys

opts = [opt for opt in sys.argv[1:] if opt.startswith("-")]
args = [arg for arg in sys.argv[1:] if not arg.startswith("-")]

def upload_code(s3_client, file_name, bucket_name):
    s3_client.upload_file(file_name, bucket_name, "jars/{}".format(file_name))


def main():

    if "-filename" in opts and "-bucket" in opts and "-awsprofile" in opts:
        file_name = args[opts.index("-filename")]

        bucket_name = args[opts.index("-bucket")]

        print("Uploading {} to bucket {}".format(file_name, bucket_name))

        awsprofile = args[opts.index("-awsprofile")]

        session = boto3.Session(profile_name=awsprofile)
       
        account_id = session.client('sts').get_caller_identity().get('Account')
        s3_client = session.client('s3')

        upload_code(s3_client, file_name, '{bucketname}-{account_id}'.format(bucketname=bucket_name, account_id=account_id))
    else:
        print("Please provide the following arguments: -filename, -bucket, -awskey, -awssecret")

if __name__ == '__main__':
    main()