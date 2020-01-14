from utils.scan_s3 import BucketScanner
import sys
import time
from pathlib import Path


def main():
    """ main function """
    streams = ["smsActivity"]
    # streams = ["calendar", "contacts", "deviceInformation", "gsmActivity", "location", "networkInformation", "smsActivity"]
    prefix = "firehose/layer-raw"
    bucket_name = "data-jumo-now-staging"
    aws_profile = "jumo-production"

    config_path = Path(__file__).resolve().parents[1] / "input" / "dates.csv"
    dates = []
    with open(config_path, "r") as file:
        for line in file:
            dates.append(line.strip())
    
    for date in dates:
        partition = f"year={date[0:4]}/month={date[4:6]}/day={date[6:8]}"
        # partition = "year=2019/month=11/day=15"
        bs = BucketScanner(aws_profile, bucket_name, prefix)
        for stream in streams:
            feed_count, metadata = bs.scan(stream, partition)
            print(f"\n\nScanned {feed_count} feeds for stream {stream} for date {date}")
            bs.extract_schema(metadata)
        time.sleep(2)
        # return

if __name__ == "__main__":
    main()
