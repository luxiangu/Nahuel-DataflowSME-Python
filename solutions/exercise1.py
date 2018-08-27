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
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToBigQuery
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
       and create a sum-per-value of users
    """

    def expand(self, game_events):
        # [START EXERCISE 1]:
        # Documentation: https://beam.apache.org/documentation/sdks/pydoc/2.5.0/
        # Developer Docs: https://beam.apache.org/documentation/programming-guide/#transforms-pardo
        # Also: https://cloud.google.com/dataflow/model/par-do

        return (game_events
                # Fill in the code to:
                # 1. Extract a tuple (user name, score) from each event
                # Hint: Lambda functions can make this very easy
                | beam.Map(lambda r: (r["user"], r["score"]))

                # 2. Compute the sum of the scores for each key.
                # You need to Combine Per Key all the elements.
                # Python has a built-in function (sum) that can combine the individual values.
                | beam.CombinePerKey(sum)
                )
        # [END EXERCISE 1]


class FormatUserScoreSumsFn(DoFn):
    """
    Formats the PCollection that ExtractAndSumScore produces in the format that BigQuery expects
    """
    def process(self, element):
        result = {
            'user': element[0],
            'total_score': element[1]
        }
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
        (p | 'ReadInputText' >> beam.io.ReadFromText(args.input)
           | 'ParseGameEvent'         >> ParDo(ParseEventFn())
           | 'ExtractUserScore'       >> ExtractAndSumScore()
           | 'FormatUserScoreSums'    >> ParDo(FormatUserScoreSumsFn())
           | 'WriteTeamScoreSums'     >> WriteToBigQuery(
                    args.output_table_name,
                    args.output_dataset,
                    options.get_all_options().get("project"),
                    table_schema()
                )
           )


if __name__ == "__main__":
    main(sys.argv)
