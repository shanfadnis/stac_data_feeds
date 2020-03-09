import json
import logging
from collections import OrderedDict
from dateutil import parser as dp
from datetime import datetime
from pathlib import Path
import re
import hashlib


LOGGER = logging.getLogger()


def load_config(payload_type):
    config_path = Path(__file__).resolve().parents[2] / "config" / payload_type / \
                  "schema.json"
    if config_path.exists():
        with open(str(config_path), 'r') as config_file:
            config = json.load(config_file)
        return config
    else:
        raise ValueError("Unknown payload type")


def parse_header(message) -> OrderedDict:
    try:
        header = OrderedDict({
                    "header_message_id": message["header"]["messageId"],
                    "header_timestamp": dp.parse(message["header"]["timestamp"]).strftime("%Y-%m-%d %H:%M:%S"),
                    "timeOfCollection": dp.parse(message["timeOfCollection"]).strftime("%Y-%m-%d %H:%M:%S"),
                    "customerIdentifier": message["customerIdentifier"],
                    "cognitoId": message["cognitoId"] if "cognitoId" in message else None
                })
    except KeyError as ke:
        logging.error("Missing field in Message Header {}".format(ke))
    except ValueError as ve:
        logging.error("Unexpected value in Message Header {}".format(ve))
    else:
        return header


def inject_checksum(item: dict) -> dict:
    """Adds checksum to our data based on the values"""
    results_ = item.copy()
    results_.pop("header_message_id")
    results_.pop("header_timestamp")
    results_.pop("timeOfCollection")
    # item["checksum"] = Sign().sign(raw_row=results_, type="md5")
    item["checksum"] = hashlib.md5(json.dumps(results_, sort_keys=True).encode("utf-8")).hexdigest()
    return item


def extract_message_payload(message: dict) -> tuple:
    """Separates the header and the payload parts of the message and checks the data type"""
    android_payload = None
    payload_type = None

    if "phoneDataType" not in message.keys():
        LOGGER.info("Message does not specify the phone data payload type")
    else:
        payload_type = message["phoneDataType"]

    if "androidPayload" not in message.keys():
        LOGGER.info("Message does not contain a data payload")
    else:
        android_payload = message["androidPayload"]

    if "customerIdentifier" not in message.keys():
        raise ValueError("Message does not contain a customer identifier")
    else:
        customer_identifier = message['customerIdentifier']

    if "timeOfCollection" not in message.keys():
        raise ValueError("Message header does not contain a timestamp")

    header = parse_header(message)

    return header, payload_type, android_payload, customer_identifier


def convert_to_snake_case(name) -> str:
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def transform_json(header, payload_type, android_payload):
    schema = load_config(payload_type)
    required_fields = [item["name"] for item in schema["fields"]]
    result = {}
    
    try:
        # HACK!!!
        temp_payload = []
        if (payload_type == "smsActivity" or payload_type == "gsmActivity") and type(android_payload) == dict:
            android_payload.pop("deviceToken", None)
            if not isinstance(android_payload, list):
                android_payload = [android_payload]
            for block in android_payload:
                for key, value in block.items():
                    temp_payload.append(block[key])
            android_payload = temp_payload
    except Exception as e:
        print(f"Error while parsing {payload_type} feed: {e}")
    
    if not isinstance(android_payload, list):
        android_payload = [android_payload]
    
    rows = []
    count = 0
    for row in android_payload:
        count += 1
        # ensure all rows have all fields
        missing_fields = list(set(required_fields).difference(set(row.keys())))
        for missing_field in missing_fields:
            row[missing_field] = None
        
        # SPECIAL CASES
        if payload_type == "location":
            if "coords" in row and row["coords"] is not None:
                for item, value in row["coords"].items():
                    if item in row:
                        row[item] = value
        
        for field in schema["fields"]:
            if field["transform"] is not None:
                name = field["transform"].get("rename", field["name"])
            else:
                name = field["name"]
            try:
                if field["transform"] is None and row[field["name"]] is not None:
                    if field["type"] == "string":
                        result[name] = str(row[field["name"]])
                    elif field["type"] == "integer":
                        result[name] = int(row[field["name"]])
                    elif field["type"] == "boolean":
                        result[name] = bool(row[field["name"]])
                    elif field["type"] == "float":
                        result[name] = float(row[field["name"]])
                    else:
                        result[name] = row[field["name"]]
                elif field["transform"] is not None and row[field["name"]] is not None:
                    if field["type"] == "datetime":
                        result[name] = dp.parse(row[field["name"]]).strftime(field["transform"]["format"])
                    elif field["type"] == "timestamp":
                        if len(str(row[field["name"]])) > 10:
                            place = len(str(row[field["name"]])) - 10
                            row[field["name"]] = float(".".join([str(row[field["name"]])[:-place],
                                                                 str(row[field["name"]])[10:]]))
                        result[name] = datetime.fromtimestamp(float(row[field["name"]]))\
                            .strftime(field["transform"]["format"])
                else:
                    result[name] = row[field["name"]]
            except ValueError:
                # result[f"__error__{field['name']}"] = f"Error while transforming {field['name']}:{row[field['name']]}"
                result[name] = None
        
        message_data = OrderedDict()
        message_data.update(header)
        message_data.update(result)
        message_data = inject_checksum(message_data)
        snake_case_dict = OrderedDict()
        for key in message_data.keys():
            snake_case_dict[convert_to_snake_case(key)] = message_data[key]
        
        rows.append(snake_case_dict)
    print(f"Processed {count} rows for {payload_type}")
    return rows


def flatten_json(message: list) -> list:
    result = []

    def flatten(block):
        if type(block) is list:
            for element in block:
                flatten(element)
        else:
            result.append(block)
    
    flatten(message)
    return result
