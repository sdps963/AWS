import boto3
import os
import configparser
import tempfile
import concurrent.futures as futures

CREDENTIALS_PATH = '~/.aws/credentials'

local_path = './shakenbake/data'
s3_src_bucket = 'cof-card-dev-partnerships-atp'
s3_src_prefix = 'gumbo/'
s3_dest_bucket = 'cof-sts-dev-gumbo-client-interface'
s3_dest_prefix = 'it/batch-direct-send/'

# read and assign aws token credentials
full_credentials_path = os.path.expanduser(CREDENTIALS_PATH)
parser = configparser.ConfigParser()
parser.read(full_credentials_path)
has_sts_prod = parser.has_section('GR_GG_COF_AWS_Card_Dev_Developer')
src_profile = 'GR_GG_COF_AWS_Card_Dev_Developer'
src_access_key = parser.get(src_profile, 'aws_access_key_id')
src_secret_key = parser.get(src_profile, 'aws_secret_access_key')
src_security_token = parser.get(src_profile, 'aws_security_token')

# create boto s3 connection object for source connection
src_session = boto3.Session(
    aws_access_key_id=src_access_key,
    aws_secret_access_key=src_secret_key,
    aws_session_token=src_security_token,
)

print("getting response from source")
source_bucket = src_session.resource('s3').Bucket(s3_src_bucket)
files = source_bucket.objects.filter(Prefix=s3_src_prefix)
source_keys = []
source_keys = [file.key for file in files if file.key != s3_src_prefix]
upload_params = {
            'ServerSideEncryption': 'AES256',
            'ACL': 'bucket-owner-full-control'
        }

def run():
    with futures.ThreadPoolExecutor(max_workers=len(source_keys)) as executor:
        jobs = [executor.submit(move_source_to_dest, source_keys[i],src_session) for i in range(len(source_keys))]
        jobs_categorize = futures.wait(jobs, timeout=6000)
        if jobs_categorize.not_done:
            print("one job didn't complete with-in time-out")
        for job in jobs_categorize.done:
            key = job.result()
            if key:
                print("Compelted {}".format(os.path.basename(key)))
                
    

def move_source_to_dest(key, src_session):
    with tempfile.TemporaryDirectory() as tempdir:
        try:
            print("downloading {}/{}".format(s3_src_bucket, key))
            src_session.client('s3').download_file(Bucket=s3_src_bucket, Key=key,
                                                   Filename=os.path.join(tempdir, os.path.basename(key)))
            print("Uploading to {}/{}".format(s3_dest_bucket, os.path.join(s3_dest_prefix, os.path.basename(key))))
            src_session.client('s3').upload_file(Filename=os.path.join(tempdir, os.path.basename(key)),
                                                  Bucket=s3_dest_bucket,
                                                  Key=os.path.join(s3_dest_prefix, os.path.basename(key)),
                                                  ExtraArgs=upload_params)
        except Exception as e:
            print("exception handling {}/{}".format(s3_src_bucket, key))
            raise e
    return key

if __name__ == "__main__":
    run()
