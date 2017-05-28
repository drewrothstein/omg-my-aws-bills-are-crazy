"""GCP Helpers."""
import logging
import time
import uuid

from google.cloud import bigquery


AWS_ACCOUNT_ID = 'XXX'
GCS_BUCKET = 'aws-bill-analyzer-data-sink'
BIGQUERY_DATASET_ID = 'aws_bill_analyzer'


def create_temporary_table(table_name):
    """Create a temporary table to load data into."""
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(BIGQUERY_DATASET_ID)
    table = dataset.table(table_name)
    table.schema = (
        bigquery.SchemaField('InvoiceID', 'STRING'),
        bigquery.SchemaField('PayerAccountId', 'STRING'),
        bigquery.SchemaField('LinkedAccountId', 'STRING'),
        bigquery.SchemaField('RecordType', 'STRING'),
        bigquery.SchemaField('RecordId', 'STRING'),
        bigquery.SchemaField('ProductName', 'STRING'),
        bigquery.SchemaField('RateId', 'STRING'),
        bigquery.SchemaField('SubscriptionId', 'STRING'),
        bigquery.SchemaField('PricingPlanId', 'STRING'),
        bigquery.SchemaField('UsageType', 'STRING'),
        bigquery.SchemaField('Operation', 'STRING'),
        bigquery.SchemaField('AvailabilityZone', 'STRING'),
        bigquery.SchemaField('ReservedInstance', 'STRING'),
        bigquery.SchemaField('ItemDescription', 'STRING'),
        bigquery.SchemaField('UsageStartDate', 'STRING'),
        bigquery.SchemaField('UsageEndDate', 'STRING'),
        bigquery.SchemaField('UsageQuantity', 'STRING'),
        bigquery.SchemaField('Rate', 'STRING'),
        bigquery.SchemaField('Cost', 'STRING'),
        bigquery.SchemaField('ResourceId', 'STRING'),
        bigquery.SchemaField('user_billing', 'STRING'),
    )
    table.create()
    logging.info('Created table: {0}'.format(table_name))


def load_bill_to_bigquery(bill_year_month, table_name):
    """Loads bill into BigQuery."""
    bill_filename = ('{0}-aws-billing-detailed-line-items-with-'
                     'resources-and-tags-{1}.csv').format(
                         AWS_ACCOUNT_ID, bill_year_month)
    load_data_from_file(BIGQUERY_DATASET_ID,
                        table_name,
                        bill_filename)


def delete_table(table_name):
    """Delete table."""
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(BIGQUERY_DATASET_ID)
    table = dataset.table(table_name)

    if table.exists():
        table.delete()
        logging.info('Deleted table: {0}'.format(table_name))
    else:
        logging.info('Table does not exist ({0}), no delete done'.format(table_name))


def copy_current_table(previous_table, new_table):
    """Copies table into better named table."""
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(BIGQUERY_DATASET_ID)
    table = dataset.table(previous_table)
    destination_table = dataset.table(new_table)
    job_id = str(uuid.uuid4())
    job = bigquery_client.copy_table(
        job_id, destination_table, table)
    job.create_disposition = (
        bigquery.job.CreateDisposition.CREATE_IF_NEEDED)
    job.begin()
    logging.info('Waiting for job to finish...')
    wait_for_job(job)
    logging.info('Table {} copied to {}.'.format(previous_table, new_table))


def load_data_from_file(dataset_name, table_name, source_file_name):
    bigquery_client = bigquery.Client()
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)

    # Reload the table to get the schema.
    table.reload()

    job_name = str(uuid.uuid4())

    job = bigquery_client.load_table_from_storage(
        job_name, table, 'gs://{0}/{1}'.format(GCS_BUCKET, source_file_name))
    
    job.begin()

    wait_for_job(job)

    logging.info('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_name, table_name))


def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)
