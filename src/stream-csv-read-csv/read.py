import boto3
import csv
import codecs
import json
import base64

# this could be a lambda handler. I believe it would deal better with constantly files being upload to the landing bucket.
#although, for reading a single, clouded big csv file, this would be ok.

s3 = boto3.client("s3")
kinesis = boto3.client("kinesis")

stream_name = "jobsity-de-challenge-kinesis-stream"
landing_bucket = "jobsity-de-challenge-landing"
csv_file = "trips.csv"

### maybe stream the whole file ???
def stream_csv(bucket, key):
    data = s3.get_object(Bucket=bucket, Key=key)

    import pdb
    pdb.set_trace()

    for row in csv.DictReader(codecs.getreader("utf-8")(data["Body"])):
        kinesis.put_record(Data=json.dumps(row).encode('utf-8'), StreamName=stream_name, PartitionKey="partitionkey")
        print(row)

if __name__ == "__main__":
    stream_csv(landing_bucket, csv_file)