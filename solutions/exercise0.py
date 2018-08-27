#
# Copyright (C) 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

#
# Zeroth (no code changes necessary) in a series of exercises in a gaming domain.
#
# This batch pipeline imports game events from CSV to BigQuery.
#
# See README.md for details.
#

import argparse
import sys

import apache_beam as beam
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
from apache_beam.options.pipeline_options import PipelineOptions
from utils.parse_event_fn import ParseEventFn


# Defines the schema for the BigQuery table where results will be stored
from utils.table_field import table_field


def table_schema():
    table_schema = TableSchema()
    table_schema.fields = [
        table_field('user', 'STRING'),
        table_field('team', 'STRING'),
        table_field('score', 'INTEGER'),
        table_field('timestamp', 'INTEGER'),
        table_field('timestamp_string', 'STRING')
    ]
    return table_schema


def main(argv):
    """Main entry point"""

    # Define and parse command line arguments
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
        type=str,
        default='',
        help='Path to the data file(s) containing game data.')

    parser.add_argument('--output_dataset',
        type=str,
        default='',
        help='The BigQuery dataset name where to write all the data.')

    parser.add_argument('--output_table_name',
        type=str,
        default='',
        help='The BigQuery table name where to write all the data.')

    args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)

    # Create and run the pipeline
    with beam.Pipeline(options=options) as p:
        (p  | 'ReadInputText' >> beam.io.ReadFromText(args.input)
            | 'ParseGameEvent' >> beam.ParDo(ParseEventFn())
            | 'WriteTeamScoreSums' >> WriteToBigQuery(
                    args.output_table_name,
                    args.output_dataset,
                    options.get_all_options().get("project"),
                    table_schema()
              )
        )
        print "-----> " + str(p.run().wait_until_finish())

if __name__ == "__main__":
    main(sys.argv)
