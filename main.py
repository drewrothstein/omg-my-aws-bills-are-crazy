#!/usr/bin/env python
"""Import AWS Bills into BigQuery from GCS"""

#
# Started with code from:
#  https://github.com/drewrothstein/simplebirdmail
#  https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/bigquery/api/load_data_from_csv.py
#
# All are licensed under Apache License, Version 2.0
#  http://www.apache.org/licenses/LICENSE-2.0
#

from datetime import datetime, timedelta
import logging
import uuid

from flask import Flask

from helpers_gcp import copy_current_table, create_temporary_table
from helpers_gcp import delete_table, load_bill_to_bigquery


app = Flask(__name__)


def runit():
    """Runs the task."""
    bills_to_parse = []

    # If date is in the first 7 days of the month, fetch last month and load
    # it too because AWS will keep writing it until past the first few days of
    # the month (provides buffer).
    now = datetime.today()
    if int(now.strftime('%-d')) <= 7:
        date_from_last_month = now - timedelta(days=7)
        bills_to_parse.append(date_from_last_month.strftime('%Y-%m'))

    # Always add current month's bill
    bills_to_parse.append(now.strftime('%Y-%m'))

    for bill_year_month in bills_to_parse:
        tmp_table_name = '_'.join(str(uuid.uuid4()).split('-'))
        bill_year_month_underscores = '_'.join(bill_year_month.split('-'))
        new_table_name = 'aws_bill_{0}'.format(bill_year_month_underscores)
        create_temporary_table(tmp_table_name)
        load_bill_to_bigquery(bill_year_month,
                              tmp_table_name)
        delete_table(new_table_name)
        copy_current_table(tmp_table_name, new_table_name)
        delete_table(tmp_table_name)

    return 'Completed'


@app.route('/run')
def run():
    return runit()


@app.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    logging.exception('An error occurred during a request.')
    return 'An internal error occurred.', 500
