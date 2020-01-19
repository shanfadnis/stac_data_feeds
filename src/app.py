from utils.scan_s3 import BucketScanner
from utils.phone_data_helpers import extract_message_payload, transform_json, flatten_json, convert_to_snake_case
from utils.db_helper import DBHelper
import sys
import json
import time
from pathlib import Path


def main():
    """ main function """
    # streams = ["gsmActivity"]
    streams = ["calendar", "contacts", "deviceInformation", "gsmActivity", "location", "networkInformation", "smsActivity"]
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
        bs = BucketScanner(aws_profile, bucket_name, prefix)
        for stream in streams:
            feed_count, metadata = bs.scan(stream, partition)
            # print(f"\nScanned {feed_count} feeds for stream {stream} for date {date}")
            feed = bs.extract_feeds(metadata)
            # bs.print_feed_schema(feeds)
            # print(feed)
            # feeds = [json.loads('[' + feed.replace("}{", "},{") + ']') for feed in feeds]
            feed = json.loads('[' + feed.replace("}{", "},{") + ']')
            # print(json.dumps(feed, indent=2))
            # print("\n===================================================================\n")
            
            transformed_rows = list()
            for message in feed:
                header, payload_type, android_payload, cid = extract_message_payload(message)
                transformed_rows.append(transform_json(header, payload_type, android_payload))
            
            flattened_rows = flatten_json(transformed_rows)
            # print(json.dumps(flattened_rows, indent=2))
            print("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

            # Write processed records into postgresql table.
            db = DBHelper()
            db.write_records(flattened_rows, f"jumo_now_{convert_to_snake_case(stream)}_payload")
    # dummy_message = [[{'a': 1}], [{'b': 2}, {'c': 3}]]
    # dummy_output = flatten_json(dummy_message)
    # print(json.dumps(dummy_output, indent=2))


if __name__ == "__main__":
    main()
