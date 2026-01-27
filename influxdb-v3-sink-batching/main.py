import os
import logging
import ast
from time import sleep
import datetime
from typing import List

# Import vendor-specific libraries
from quixstreams import Application
import influxdb_client_3 as InfluxDBClient3

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Kafka Configuration
KAFKA_BROKER_ADDRESS = os.getenv("KAFKA_BROKER_ADDRESS")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "influxdb-v2-data")

# InfluxDB v3 Configuration
INFLUXDB_HOST = os.getenv("INFLUXDB_HOST")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_TAG_KEYS = os.getenv("INFLUXDB_TAG_KEYS", "[]")
INFLUXDB_FIELD_KEYS = os.getenv("INFLUXDB_FIELD_KEYS", "[]")

# Performance Tuning Parameters
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))  # Reduced from 2000 for stability
BATCH_TIMEOUT = float(os.getenv("BATCH_TIMEOUT", "5.0"))  # Reduced from 10s
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))  # Retry attempts for failed writes

# ============================================================================
# SETUP
# ============================================================================

# Parse tag and field keys
tag_keys = ast.literal_eval(INFLUXDB_TAG_KEYS)
field_keys = ast.literal_eval(INFLUXDB_FIELD_KEYS)

# Create Quix Application with optimized settings
app = Application(
    broker_address=KAFKA_BROKER_ADDRESS,
    consumer_group="influxdb-v3-writer",
    auto_offset_reset="earliest",
    auto_create_topics=True,
    commit_interval=2.0,  # Commit offsets every 2 seconds
    commit_every=500,  # Or every 500 messages processed
)

input_topic = app.topic(
    name=INPUT_TOPIC,
    key_serializer="string",
    value_serializer="json",
)

# Initialize InfluxDB v3 client
influxdb3_client = InfluxDBClient3.InfluxDBClient3(
    host=INFLUXDB_HOST,
    org=INFLUXDB_ORG,
    token=INFLUXDB_TOKEN,
    database=INFLUXDB_BUCKET,
)

# ============================================================================
# STATE MANAGEMENT
# ============================================================================

# Batch storage
points_batch: List[dict] = []
last_write_time = datetime.datetime.utcnow()
total_written = 0
total_failed = 0

# ============================================================================
# CORE FUNCTIONS
# ============================================================================

def prepare_point(message: dict) -> dict:
    """
    Convert a Kafka message to an InfluxDB point format.
    
    Args:
        message: Dictionary containing the data point
        
    Returns:
        Dictionary in InfluxDB point format or None if error
    """
    try:
        # Use original timestamp if available, otherwise use current time
        if "original_time" in message:
            writetime = message["original_time"]
        else:
            writetime = datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
        
        measurement_name = message["_measurement"]

        # Build tags dictionary from configured tag keys
        tags = {tag_key: message[tag_key] for tag_key in tag_keys if tag_key in message}
        
        # Build fields dictionary from configured field keys
        fields = {field_key: message[field_key] for field_key in field_keys if field_key in message}

        return {
            "measurement": measurement_name,
            "tags": tags,
            "fields": fields,
            "time": writetime
        }
    except Exception as e:
        logger.error(f"Error preparing point: {e}, message: {message}")
        return None


def flush_batch():
    """
    Write accumulated points to InfluxDB v3 with retry logic.
    Uses exponential backoff for retries.
    """
    global points_batch, last_write_time, total_written, total_failed
    
    if not points_batch:
        return
    
    batch_to_write = points_batch.copy()
    batch_size = len(batch_to_write)
    
    for attempt in range(MAX_RETRIES):
        try:
            start_time = datetime.datetime.utcnow()
            
            # Write batch to InfluxDB v3
            influxdb3_client.write(record=batch_to_write, write_precision="ms")
            
            end_time = datetime.datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            total_written += batch_size
            
            # Log success
            logger.info(
                f"✓ Wrote {batch_size} points in {duration:.2f}s "
                f"({batch_size/duration:.0f} pts/sec) | "
                f"Total: {total_written:,} | Failed: {total_failed:,}"
            )
            
            # Success - clear batch and return
            points_batch = []
            last_write_time = datetime.datetime.utcnow()
            return
            
        except Exception as e:
            logger.error(f"Batch write failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            
            if attempt < MAX_RETRIES - 1:
                # Exponential backoff: 1s, 2s, 4s
                sleep_time = 2 ** attempt
                logger.info(f"Retrying in {sleep_time}s...")
                sleep(sleep_time)
            else:
                # All retries exhausted
                total_failed += batch_size
                logger.error(
                    f"❌ Failed to write batch of {batch_size} points after {MAX_RETRIES} attempts! "
                    f"Total failed: {total_failed:,}"
                )
                # Clear batch to avoid infinite loop
                points_batch = []
                # Re-raise exception to let Quix handle it
                raise


def process_message(message):
    """
    Process incoming Kafka message and add to batch.
    Flushes batch when size or timeout threshold is reached.
    """
    global points_batch, last_write_time
    
    # Convert message to InfluxDB point
    point = prepare_point(message)
    if point:
        points_batch.append(point)
    else:
        logger.warning("Failed to prepare point, skipping")
        return
    
    # Check if we should flush the batch
    time_since_last_write = (datetime.datetime.utcnow() - last_write_time).total_seconds()
    
    if len(points_batch) >= BATCH_SIZE or time_since_last_write >= BATCH_TIMEOUT:
        flush_batch()
    
    # Safety check: if batch is getting too large, force flush
    if len(points_batch) > BATCH_SIZE * 1.5:
        logger.warning(f"Batch size exceeded safe limit ({len(points_batch)}), forcing flush")
        flush_batch()


# ============================================================================
# STREAM PROCESSING
# ============================================================================

sdf = (
    app
        .dataframe(input_topic)
        .update(process_message)
)

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    logger.info("=" * 70)
    logger.info("Starting InfluxDB v3 Sink with Batching")
    logger.info("=" * 70)
    logger.info(f"Kafka Broker: {KAFKA_BROKER_ADDRESS}")
    logger.info(f"Input Topic: {INPUT_TOPIC}")
    logger.info(f"InfluxDB Host: {INFLUXDB_HOST}")
    logger.info(f"Target Database: {INFLUXDB_BUCKET}")
    logger.info(f"Batch Size: {BATCH_SIZE}")
    logger.info(f"Batch Timeout: {BATCH_TIMEOUT}s")
    logger.info(f"Max Retries: {MAX_RETRIES}")
    logger.info(f"Tag Keys: {tag_keys}")
    logger.info(f"Field Keys: {field_keys}")
    logger.info("=" * 70)
    
    # Add health check logging
    import psutil
    process = psutil.Process()
    
    def log_health():
        mem_info = process.memory_info()
        logger.info(f"Health: Memory={mem_info.rss / 1024 / 1024:.1f}MB, Batch={len(points_batch)}")
    
    try:
        # Log initial health
        log_health()
        app.run()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received")
    except Exception as e:
        logger.error(f"CRITICAL ERROR: {type(e).__name__}: {e}")
        logger.error(f"Last known state - Written: {total_written:,}, Batch size: {len(points_batch)}")
        # Don't suppress the error - let it crash so we can see what's wrong
        raise
    finally:
        # Flush any remaining points on shutdown
        logger.info("Flushing remaining points...")
        try:
            flush_batch()
        except Exception as e:
            logger.error(f"Error flushing final batch: {e}")
        logger.info(f"Final Stats - Written: {total_written:,} | Failed: {total_failed:,}")
        log_health()
        logger.info("Application stopped")