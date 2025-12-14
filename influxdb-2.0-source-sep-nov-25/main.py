# Import basic utilities
import os
import random
import json
import logging
from time import sleep
from datetime import datetime, timedelta

# import vendor-specfic modules
from quixstreams import Application
from quixstreams.models.serializers.quix import JSONSerializer, SerializationContext
import influxdb_client

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a Quix Application
app = Application()

# Define a serializer for messages, using JSON Serializer for ease
serializer = JSONSerializer()

# Define the topic using the "output" environment variable
topic_name = os.environ["output"]
topic = app.topic(topic_name)

influxdb2_client = influxdb_client.InfluxDBClient(
    token=os.environ["INFLUXDB_TOKEN"],
    org=os.environ["INFLUXDB_ORG"],
    url=os.environ['INFLUXDB_HOST']
    # timeout=120_000  # 5 minutes in milliseconds
)

query_api = influxdb2_client.query_api()

interval = os.environ.get("task_interval", "5m")
bucket = os.environ.get("INFLUXDB_BUCKET", "placeholder-bucket")

# Backfill settings
backfill_enabled = os.environ.get("BACKFILL_ENABLED", "true").lower() == "true"
backfill_start = os.environ.get("BACKFILL_START", "2025-05-16")     # Can be YYYY-MM-DD or -60d format
backfill_end = os.environ.get("BACKFILL_END", "2025-06-01")         # Optional: YYYY-MM-DD format, empty means "now"
backfill_chunk_size = os.environ.get("BACKFILL_CHUNK_SIZE", "1h")

# Global variable to control the main loop's execution
run = True

# Helper function to convert time intervals (like 1h, 2m) into seconds for easier processing.
# This function is useful for determining the frequency of certain operations.
UNIT_SECONDS = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
    "y": 31536000,
}

def interval_to_seconds(interval: str) -> int:
    try:
        return int(interval[:-1]) * UNIT_SECONDS[interval[-1]]
    except ValueError as e:
        if "invalid literal" in str(e):
            raise ValueError(
                "interval format is {int}{unit} i.e. '10h'; "
                f"valid units: {list(UNIT_SECONDS.keys())}")
    except KeyError:
        raise ValueError(
            f"Unknown interval unit: {interval[-1]}; "
            f"valid units: {list(UNIT_SECONDS.keys())}")

interval_seconds = interval_to_seconds(interval)

def interval_to_timedelta(interval: str) -> timedelta:
    """Convert interval string to timedelta object"""
    unit = interval[-1]
    value = int(interval[:-1])
    
    if unit == 's':
        return timedelta(seconds=value)
    elif unit == 'm':
        return timedelta(minutes=value)
    elif unit == 'h':
        return timedelta(hours=value)
    elif unit == 'd':
        return timedelta(days=value)
    elif unit == 'w':
        return timedelta(weeks=value)
    elif unit == 'y':
        return timedelta(days=value * 365)
    else:
        raise ValueError(f"Unknown unit: {unit}")

def is_dataframe(result):
    return type(result).__name__ == 'DataFrame'

def query_influx_range(start_time, end_time):
    """Query InfluxDB for a specific time range"""
    flux_query = f'''
    from(bucket: "{bucket}")
        |> range(start: {start_time}, stop: {end_time})
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    logger.info(f"Querying range: {start_time} to {end_time}")
    
    try:
        table = query_api.query_data_frame(query=flux_query, org=os.environ['INFLUXDB_ORG'])
        
        # If the query returns tables with different schemas, result will be a list of dataframes
        if isinstance(table, list):
            for item in table:
                if len(item) > 0:
                    item.rename(columns={'_time': 'original_time'}, inplace=True)
                    json_result = item.to_json(orient='records', date_format='iso')
                    yield json_result
                    logger.info(f"Published {len(item)} rows from multiple measurements")
        elif is_dataframe(table) and len(table) > 0:
            table.rename(columns={'_time': 'original_time'}, inplace=True)
            json_result = table.to_json(orient='records', date_format='iso')
            yield json_result
            logger.info(f"Published {len(table)} rows from single measurement")
        else:
            logger.info("No results for this time range")
            
    except Exception as e:
        logger.error(f"Query failed for range {start_time} to {end_time}: {e}")
        raise


def backfill_historical_data():
    """Backfill historical data in chunks"""
    logger.info(f"Starting backfill from {backfill_start} in {backfill_chunk_size} chunks")
    
    # Calculate time ranges
    now = datetime.utcnow()
    
    # Parse start time - support both date format and interval format
    if backfill_start.startswith('-'):
        # Relative format like "-60d"
        start_delta = interval_to_timedelta(backfill_start.replace('-', ''))
        start_time = now - start_delta
    else:
        # Absolute date format like "2025-01-01"
        try:
            start_time = datetime.strptime(backfill_start, '%Y-%m-%d')
        except ValueError:
            try:
                start_time = datetime.strptime(backfill_start, '%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                logger.error(f"Invalid BACKFILL_START format: {backfill_start}. Use YYYY-MM-DD or -XXd")
                return

    # Parse end time - if specified, use it; otherwise use now
    if backfill_end:
        try:
            end_time = datetime.strptime(backfill_end, '%Y-%m-%d')
        except ValueError:
            try:
                end_time = datetime.strptime(backfill_end, '%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                logger.error(f"Invalid BACKFILL_END format: {backfill_end}. Use YYYY-MM-DD")
                return
        logger.info(f"Using specified end time: {end_time}")
    else:
        end_time = now
        logger.info(f"No end time specified, using current time: {end_time}")
    

    chunk_delta = interval_to_timedelta(backfill_chunk_size)
    current_time = start_time
    
    logger.info(f"Backfilling from {start_time} to {end_time} ({(end_time - start_time).days} days)")
    
    chunk_count = 0
    total_chunks = int((end_time - start_time).total_seconds() / chunk_delta.total_seconds())
    
    while current_time < end_time:
        chunk_end_time = min(current_time + chunk_delta, end_time)
        chunk_count += 1
        
        # Format times for Flux query
        start_str = current_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_str = chunk_end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        logger.info(f"Processing chunk {chunk_count}/{total_chunks}: {start_str} to {end_str}")
        
        try:
            for result in query_influx_range(start_str, end_str):
                yield result
        except Exception as e:
            logger.error(f"Failed to backfill chunk {start_str} to {end_str}: {e}")
            # Continue with next chunk even if this one fails
        
        current_time = chunk_end_time
        sleep(1)  # Small delay between chunks to avoid overwhelming the server
    
    logger.info(f"Backfill completed: processed {chunk_count} chunks")

# def is_dataframe(result):
#     return type(result).__name__ == 'DataFrame'

# Function to fetch data from InfluxDB and send it to Quix
# It runs in a continuous loop, periodically fetching data based on the interval.
def get_data():
    # Run in a loop until the main thread is terminated

    # If backfill is enabled, do that first
    if backfill_enabled:
        logger.info("Backfill mode enabled")
        for result in backfill_historical_data():
            yield result
        logger.info("Backfill completed, switching to continuous mode")
    

def main():
    """
    Read data from the Query and publish it to Kafka
    """

    # Create a pre-configured Producer object.
    # Producer is already setup to use Quix brokers.
    # It will also ensure that the topics exist before producing to them if
    # Application.Quix is initialized with "auto_create_topics=True".
    producer = app.get_producer()

    with producer:
        for res in get_data():
            # Parse the JSON string into a Python object
            records = json.loads(res)
            for index, obj in enumerate(records):
                # Generate a unique message_key for each row
                message_key = f"INFLUX2_DATA_{str(random.randint(1, 100)).zfill(3)}_{index}"
                logger.info(f"Produced message with key:{message_key}, value:{obj}")

                # Serialize row value to bytes
                serialized_value = serializer(
                    value=obj, ctx=SerializationContext(topic=topic.name, field="value")
                )

                # publish the data to the topic
                producer.produce(
                    topic=topic.name,
                    key=message_key,
                    value=serialized_value,
                )

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Exiting.")