# IXP Collector

This module collects and processes IXP (Internet Exchange Point) data from PeeringDB and integrates it into the HERMES enrichment pipeline.

## Overview

The IXP collector performs the following tasks:
1. Runs a wrapper script to generate IXP member data
2. Processes the generated data file
3. Inserts the data into two BigQuery tables:
   - `ix_data.ixp_members`: Historical IXP membership data
   - `hermes.ixp_mapping`: Current IXP mapping for enrichment

## Dependencies

- Python 3.7+
- Google Cloud BigQuery client library
- tqdm for progress bars

## Usage

### As a Standalone Script

```python
from peeringdb_ixp.ixp_collector import update_ixp_data

success = update_ixp_data()
```

### As Part of HERMES Pipeline

The `update_ixp_data()` function can be called as part of the HERMES enrichment pipeline to keep IXP data up to date.

## Configuration

The IXP collector can be configured through the `IXPCollector` class initialization:

```python
collector = IXPCollector(
    project_id="your-project-id",
    wrapper_script_path="/path/to/wrapper.py",
    python_executable="/path/to/python",
    output_dir="/path/to/output",
    batch_size=1000
)
```

## Data Structure

### Input Data Format

The wrapper script generates a tab-separated file with the following format:
```
IPv4_Address    ASN    IXP_Name
```

### BigQuery Tables

#### ix_data.ixp_members
- `asn`: ASN number (INTEGER)
- `ipv4`: IPv4 address (STRING)
- `name`: IXP name (STRING)
- `partition_date`: Date of data collection (DATE)

#### hermes.ixp_mapping
- `asn`: ASN number (INTEGER)
- `ipv4`: IPv4 address (STRING)
- `ixp_name`: IXP name (STRING)
- `last_updated`: Last update timestamp (TIMESTAMP)

## Error Handling

The collector includes comprehensive error handling and logging:
- Wrapper script execution errors
- File processing errors
- BigQuery insertion errors

All errors are logged with appropriate context and severity levels.

## Integration

To integrate with the HERMES enrichment pipeline:

1. Import the update function:
```python
from hermes.enrichment.peeringdb_ixp.ixp_collector import update_ixp_data
```

2. Call the function in your pipeline:
```python
def run_enrichment_pipeline():
    # ... other enrichment steps ...
    if not update_ixp_data():
        logger.error("IXP data update failed")
        return False
    # ... continue with pipeline ...
``` 