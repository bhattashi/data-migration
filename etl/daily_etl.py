import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import csv
from io import StringIO
import logging
import datetime # <-- NEW: Required for robust timestamp generation

# Set up logging for better visibility during execution
logging.basicConfig(level=logging.INFO)

# --- 1. Define the BigQuery Schema ---
# This dictionary defines the structure and types of the output table in BigQuery.
BIGQUERY_SCHEMA = {
    'fields': [
        {'name': 'transaction_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'customer_name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'transaction_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        {'name': 'processed_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    ]
}

# --- 2. Transformation Function (The T in ETL) ---
def parse_and_transform(line):
    """
    Parses a CSV line, applies a business filter, and transforms the data.
    """
    reader = csv.reader(StringIO(line))
    try:
        row = next(reader) 
        
        # Row must have exactly 4 fields for safety
        if len(row) != 4:
            logging.warning(f"Skipping record: Incorrect field count (expected 4): {line}")
            return None

        # Simple Validation/Filtering: Skip records that are NOT 'SUCCESS'
        if row[2] != 'SUCCESS':
            # This filters out 'PENDING' transactions and the header row if ReadFromText failed
            return None 
            
        # Extract and transform fields. Conversions must succeed here.
        return {
            # Map input columns to BigQuery schema names and types
            'transaction_id': int(row[0]),
            'customer_name': row[1].upper(), # Transformation: Convert name to uppercase
            'transaction_amount': float(row[3]),
            # NEW ROBUST TIMESTAMP: Use Python's datetime.now() converted to ISO format
            'processed_timestamp': datetime.datetime.now().isoformat()
        }
    except ValueError as ve:
        # Catch specific errors (like trying to convert 'id' or 'amount' string to int/float)
        # This catches the header row if skip_header_lines=0 was used or failed.
        logging.warning(f"Skipping bad record (Conversion Error: {ve}): {line}")
        return None
    except Exception as e:
        # Catch any other unexpected error and skip the record
        logging.error(f"FATAL error processing record: {line}. Error: {e}")
        return None

# To ensure the template captures the parameters, the options class
# should be defined at the top level or passed correctly to the runner.
class MyTemplateOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # ValueProvider arguments are the only ones visible to gcloud/Airflow at runtime
        parser.add_value_provider_argument('--input_file', help='GCS path to the input file.')
        parser.add_value_provider_argument('--output_table', help='BigQuery output table.')

# --- 3. Pipeline Definition ---

def run_pipeline(argv=None):
    """Builds and runs the Dataflow pipeline."""
    # Initialize with the custom options class explicitly
    # ADDED: save_main_session=True ensures the global context is available on workers
    pipeline_options = PipelineOptions(argv, save_main_session=True)
    custom_options = pipeline_options.view_as(MyTemplateOptions)

    # Parse command line arguments
    # parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     '--input_file',
    #     required=True,
    #     help='GCS path to the input CSV file (e.g., gs://bucket/data.csv)')
    # parser.add_argument(
    #     '--output_table',
    #     required=True,
    #     help='BigQuery output table (e.g., project:dataset.table_name)')
    
    # known_args, pipeline_args = parser.parse_known_args(argv)

    # # Define the pipeline options (Runner, Project, Region, etc.)
    # # We explicitly set the runner to DataflowRunner for deployment
    # pipeline_args.append('--runner=DataflowRunner')
    # pipeline_options = PipelineOptions(pipeline_args)

    logging.info(f"Starting Dataflow job. Reading from {custom_options.input_file}")
    # We MUST pass the pipeline_options to the Pipeline object
    with beam.Pipeline(options=pipeline_options) as p:
        
        # Read the input file from GCS, explicitly skipping the header
        lines = p | 'ReadFromGCS' >> beam.io.ReadFromText(custom_options.input_file, skip_header_lines=1)
        
        # Apply the transformation (parsing, filtering, and mapping)
        transformed_data = (
            lines
            | 'ParseAndTransform' >> beam.Map(parse_and_transform)
            | 'FilterNone' >> beam.Filter(lambda x: x is not None)
        )
        
        # Write the transformed PCollection to BigQuery
        transformed_data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            custom_options.output_table,
            schema=BIGQUERY_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE, # Overwrite the table each run
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
        
        logging.info("Pipeline definition complete.")

if __name__ == '__main__':
    run_pipeline()
