import os
import time
from datetime import datetime, timedelta
from quixstreams import Application
from influxdb_client import InfluxDBClient
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# InfluxDB v2 Configuration
INFLUXDB_URL = os.environ.get("INFLUXDB_URL", "http://influxdb-v2:8086")
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.environ.get("INFLUXDB_ORG", "")
INFLUXDB_BUCKET = os.environ.get("INFLUXDB_BUCKET", "")

# Query Configuration
MEASUREMENT = os.environ.get("MEASUREMENT", "*")  # Default to all measurements
QUERY_INTERVAL_SECONDS = int(os.environ.get("QUERY_INTERVAL_SECONDS", "60"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
START_TIME = os.environ.get("START_TIME", "-7d")  # Default: last 7 days

# Quix Configuration
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "influxdb-data")

def create_influxdb_client():
    """Create and return InfluxDB client"""
    try:
        client = InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG
        )
        # Test connection
        health = client.health()
        logger.info(f"InfluxDB connection successful. Status: {health.status}")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to InfluxDB: {e}")
        raise

def build_flux_query(start_time, measurement, batch_size):
    """Build Flux query for data extraction"""
    if measurement == "*":
        query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: {start_time})
            |> limit(n: {batch_size})
        '''
    else:
        query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
            |> range(start: {start_time})
            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
            |> limit(n: {batch_size})
        '''
    return query

def query_influxdb_data(client, start_time, measurement, batch_size):
    """Query data from InfluxDB v2"""
    try:
        query_api = client.query_api()
        query = build_flux_query(start_time, measurement, batch_size)
        
        logger.info(f"Executing query: {query}")
        tables = query_api.query(query, org=INFLUXDB_ORG)
        
        records = []
        for table in tables:
            for record in table.records:
                data_point = {
                    "measurement": record.get_measurement(),
                    "time": record.get_time().isoformat(),
                    "field": record.get_field(),
                    "value": record.get_value(),
                    "tags": record.values.copy()
                }
                # Remove non-tag fields from tags dict
                for key in ["_start", "_stop", "_time", "_value", "_field", "_measurement", "result", "table"]:
                    data_point["tags"].pop(key, None)
                
                records.append(data_point)
        
        logger.info(f"Retrieved {len(records)} records from InfluxDB")
        return records
    
    except Exception as e:
        logger.error(f"Error querying InfluxDB: {e}")
        return []

def main():
    """Main application loop"""
    logger.info("Starting InfluxDB v2 to Quix Source Application")
    
    # Validate configuration
    if not INFLUXDB_TOKEN or not INFLUXDB_ORG or not INFLUXDB_BUCKET:
        logger.error("Missing required InfluxDB configuration. Please set INFLUXDB_TOKEN, INFLUXDB_ORG, and INFLUXDB_BUCKET")
        return
    
    # Create InfluxDB client
    influx_client = create_influxdb_client()
    
    # Create Quix application
    app = Application()
    output_topic = app.topic(OUTPUT_TOPIC)
    
    logger.info(f"Publishing to topic: {OUTPUT_TOPIC}")
    logger.info(f"Query interval: {QUERY_INTERVAL_SECONDS} seconds")
    logger.info(f"Measurement filter: {MEASUREMENT}")
    
    try:
        with app.get_producer() as producer:
            while True:
                try:
                    # Query data from InfluxDB
                    records = query_influxdb_data(
                        influx_client,
                        START_TIME,
                        MEASUREMENT,
                        BATCH_SIZE
                    )
                    
                    # Publish records to Quix
                    for record in records:
                        message = output_topic.serialize(
                            key=record["measurement"],
                            value=record
                        )
                        producer.produce(
                            topic=output_topic.name,
                            key=message.key,
                            value=message.value
                        )
                    
                    if records:
                        producer.flush()
                        logger.info(f"Published {len(records)} records to Quix topic")
                    else:
                        logger.info("No new records to publish")
                    
                    # Wait before next query
                    time.sleep(QUERY_INTERVAL_SECONDS)
                
                except Exception as e:
                    logger.error(f"Error in processing loop: {e}")
                    time.sleep(10)  # Wait before retrying
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        influx_client.close()
        logger.info("Application stopped")

if __name__ == "__main__":
    main()