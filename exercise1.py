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

import argparse
import sys

import apache_beam as beam
from apache_beam import ParDo, PTransform, DoFn
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema

from apache_beam.options.pipeline_options import PipelineOptions

from utils.change_me import ChangeMe
from utils.parse_event_fn import ParseEventFn


# Defines the schema for the BigQuery table where results will be stored
from utils.table_field import table_field


def table_schema():
    table_schema = TableSchema()
    table_schema.fields = [
        table_field('total_score', 'INTEGER'),
        table_field('user', 'STRING')
    ]
    return table_schema


class ExtractAndSumScore(PTransform):
    """
    Implement a series of PTransforms that take a PCollection of events
       and create a sum-per-value of a given field
    """

    def __init__(self, field):
        super(PTransform, self)
        self.field = field

    def expand(self, game_events):
        # [START EXERCISE 1]:
        # Documentation: https://beam.apache.org/documentation/sdks/pydoc/2.4.0/
        # Developer Docs: https://beam.apache.org/documentation/programming-guide/#transforms-pardo
        # Also: https://cloud.google.com/dataflow/model/par-do

        return (game_events

            # Fill in the code to:
            # 1. Extract a tuple (String, Integer) from each event
            #    corresponding to the given field and the score.
            # Hint: you can use a lambda here to make your code more compact
            | beam.Map(ChangeMe())

            # 2. Compute the sum of the scores for each key.
            # You need to Combine Per Key all the elements for each key
            # Python has a built-in sum function that can combine the individual values
            | ChangeMe()
        )
        # [END EXERCISE 1]


class FormatUserScoreSumsFn(DoFn):
    """
    Formats the PCollection that ExtractAndSumScore produces in the format that BigQuery expects
    """

    def __init__(self):
        super(DoFn, self)

    def process(self, element):
        result = {
            'user': element[0],
            'total_score': element[1]
        }
        print (str(result) + "\n")
        yield result


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
        from apache_beam.io import WriteToBigQuery
        (p
         | 'ReadInputText'          >> beam.io.ReadFromText(args.input)
         | 'ParseGameEvent'         >> ParDo(ParseEventFn())
         | 'ExtractUserScore'       >> ExtractAndSumScore("user")
         | 'FormatUserScoreSums'    >> ParDo(FormatUserScoreSumsFn())

        # This code is disabled due to the same bug that blocked BEAM-3824
        # | 'WriteTeamScoreSums'     >> WriteToBigQuery(
        #            args.output_table_name,
        #            args.output_dataset,
        #            options.get_all_options().get("project"),
        #            table_schema()
        #        )
        # Once WriteToBigQuery is fixed, replace the code below with the one above.
         | 'WriteTeamScoreSums'     >> beam.io.Write(beam.io.BigQuerySink(
                    (options.get_all_options().get("project")
                        + ":" + args.output_dataset
                        + "." + args.output_table_name),
                    schema=table_schema(),
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
         )

if __name__ == "__main__":
    main(sys.argv)
