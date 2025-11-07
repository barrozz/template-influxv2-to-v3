import os
import random
import json
import logging
import ast
from time import sleep
import datetime

# Import vendor-specific libraries
from quixstreams import Application
import influxdb_client_3 as InfluxDBClient3

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize parameters from environment
KAFKA_BROKER_ADDRESS = os.getenv("KAFKA_BROKER_ADDRESS")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "influxdb-v2-data")
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_TAG_KEYS = os.getenv("INFLUXDB_TAG_KEYS", "[]")
INFLUXDB_FIELD_KEYS = os.getenv("INFLUXDB_FIELD_KEYS", "[]")

# Create a Quix Application
app = Application(
    broker_address=KAFKA_BROKER_ADDRESS,
    consumer_group="influxdb-v3-writer",
    auto_offset_reset="earliest",
    auto_create_topics=True,
)

input_topic = app.topic(
    name=INPUT_TOPIC,
    key_serializer="string",
    value_serializer="json",
)

# Read the environment variable and convert it to a dictionary
tag_keys = ast.literal_eval(INFLUXDB_TAG_KEYS)
field_keys = ast.literal_eval(INFLUXDB_FIELD_KEYS)

influxdb3_client = InfluxDBClient3.InfluxDBClient3(
    host=INFLUXDB_HOST,
    org=INFLUXDB_ORG,
    token=INFLUXDB_TOKEN,
    database=INFLUXDB_BUCKET,
)


def send_data_to_influx(message):
    logger.info(f"Processing message: {message}")
    try:
        # Uses the current time as the timestamp for writing to the sink
        # Adjust to use an alternative timestamp if necesssary,

        # writetime = datetime.datetime.utcnow()
        # writetime = writetime.isoformat(timespec="milliseconds") + "Z"

        if "original_time" in message:
            writetime = message["original_time"]
            logger.info(f"Using original timestamp: {writetime}")
        else:
            writetime = datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
            logger.warning(f"No original_time found, using current time: {writetime}")
        
        measurement_name = message["_measurement"]

        # Initialize the tags and fields dictionaries
        tags = {}
        fields = {}

        # Iterate over the tag_dict and field_dict to populate tags and fields
        for tag_key in tag_keys:
            if tag_key in message:
                tags[tag_key] = message[tag_key]

        for field_key in field_keys:
            if field_key in message:
                fields[field_key] = message[field_key]

        logger.info(f"Using tag keys: {', '.join(tags.keys())}")
        logger.info(f"Using field keys: {', '.join(fields.keys())}")

        # Construct the points dictionary
        points = {
            "measurement": measurement_name,
            "tags": tags,
            "fields": fields,
            "time": writetime
        }

        logger.info(f"MEASUREMENT: {measurement_name}, TAGS: {tags}, FIELDS: {fields}, TIME: {writetime}")

        influxdb3_client.write(record=points, write_precision="ms")
        
        print(f"{str(datetime.datetime.utcnow())}: Persisted ponts to influx: {points}")
    except Exception as e:
        print(f"{str(datetime.datetime.utcnow())}: Write failed")
        print(e)


sdf = (
    app
        .dataframe(input_topic)
        .update(send_data_to_influx)
)

if __name__ == "__main__":
    print("Starting application")
    app.run(sdf)
