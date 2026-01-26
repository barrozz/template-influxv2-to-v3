# InfluxDB v3 Sink
A service that continuously listens to a Kafka topic and writes new data to an InfluxDB v3 bucket, only including the configured tags and fields.

Requires the following environment variables to be set:

| Variable   |      Description      |  Example |
|----------|---------------------------------------|------|
| `INPUT_TOPIC` | The input topic from which to consume the InfluxDB v2 data | `influxdb-v2-data` |
| `INFLUXDB_HOST` | The address of the InfluxDB v3 instance | `https://us-east-1-2.aws.cloud2.influxdata.com` |
| `INFLUXDB_ORG` |  The configured organization in the InfluxDB v3 instance | `AcmeInc` |
| `INFLUXDB_BUCKET` | The name of the InfluxDB v3 bucket to store the synced data |   `machine-telemetry-v3` |
| `INFLUXDB_TOKEN` | The configured API Token to access the InfluxDB v3 instance (defined as a secret in Quix) |   `Rm3545345357qnv-gOX54346346EHr-g1YSB79T29w_5VdwEuXWK6gg535g34232yDX_VAYfA33RFd4Xw==` |
| `INFLUXDB_TAG_KEYS` | A list of tags to select when when writing the data to InfluxDB v3 (all others will be ignored) |  `['machineID','barcode','provider']` |
| `INFLUXDB_FIELD_KEYS` | A list of fields to select when when writing the data to InfluxDB v3 (all others will be ignored) |  `['temperature','load','power','vibration']` |
