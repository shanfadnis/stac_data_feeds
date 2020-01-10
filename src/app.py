from utils.scan_s3 import BucketScanner
import sys

def main(year, month, day):
    """ main function """
    streams = ["calendar", "contacts", "deviceInformation", "gsmActivity", "location", "networkInformation", "smsActivity"]
    partition = f"year={year}/month={month}/day={day}"
    prefix = "firehose/layer-raw"
    bucket_name = "data-jumo-now-staging"
    aws_profile = "jumo-production"
    bs = BucketScanner(aws_profile, bucket_name, prefix)
    for stream in streams:
        feed_count, metadata = bs.scan(stream, partition)
        print("\n\nScanned {} feeds for stream {}".format(feed_count, stream))
        bs.extract_schema(metadata)

if __name__ == "__main__":
    year, month, day = sys.argv[1:4]
    main(year, month, day)