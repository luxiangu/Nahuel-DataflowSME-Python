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
from apache_beam import ParDo, PTransform, DoFn, WindowInto
from apache_beam.io import Write, WriteToBigQuery
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema

from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms.window import TimestampedValue, FixedWindows

from utils.change_me import ChangeMe
from utils.parse_event_fn import ParseEventFn

# Defines the schema for the BigQuery table where results will be stored
from utils.table_field import table_field


#
# Second in a series of coding exercises in a gaming domain.
#
# This batch pipeline calculates the sum of scores per team per hour, over an entire batch of
# gaming data and writes the per-team sums to BigQuery.
#
# See README.md for details.
#

def table_schema():
    table_schema = TableSchema()
    table_schema.fields = [
        table_field('team', 'STRING'),
        table_field('total_score', 'INTEGER'),
        table_field('window_start', 'TIMESTAMP')
    ]
    return table_schema

class ExtractAndSumScore(PTransform):
    def __init__(self, field):
        super(PTransform, self)
        self.field = field

    def expand(self, game_events):
        return (game_events
                | beam.Map(lambda e: (e[self.field], e["score"]))
                | beam.CombinePerKey(sum)
                )


#
# A transform to compute the WindowedTeamScore.
#
class WindowedTeamScore (PTransform):
    # Developer Docs for composite transforms:
    # https://beam.apache.org/documentation/programming-guide/#transforms-composite

    def __init__(self, duration):
        self.duration = duration;

    def expand(self, input):
        # [START EXERCISE 2]:
        # Docs: https://beam.apache.org/documentation/sdks/pydoc/2.5.0/
        # Developer Docs: https://beam.apache.org/documentation/programming-guide/#windowing
        # Also: https://cloud.google.com/dataflow/model/windowing

        return (input
                # WindowInto() takes a WindowFn and returns a PTransform that applies windowing to the PCollection.
                # FixedWindows() returns a WindowFn that assigns elements to windows of a fixed number of milliseconds.
                # Use these methods to apply fixed windows of size self.duration to the PCollection.
                | WindowInto(FixedWindows(self.duration))
                # Remember the ExtractAndSumScore PTransform from Exercise 1?
                # We parameterized it over the key field (see below). Use it here to compute the "team" scores
                | ExtractAndSumScore("team")
                # [END EXERCISE 2]
                )

#
# Format a KV of team and their score to a BigQuery Table row
#
class FormatTeamScoreSumsFn (DoFn):
    """
    Formats the PCollection that ExtractAndSumScore produces in the format that BigQuery expects
    """

    def process(self, element, window=beam.DoFn.WindowParam):
        row = {
            'team': element[0],
            'total_score': element[1],
            'window_start': window.start.micros / 1000000000
        }
        yield row


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
    options.view_as(SetupOptions).save_main_session = True

    # Create and run the pipeline
    with beam.Pipeline(options=options) as p:
        (p | 'ReadInputText'          >> beam.io.ReadFromText(args.input)
           | 'ParseGameEvent'         >> ParDo(ParseEventFn())
           | 'AddEventTimestamps'     >> beam.Map(lambda element: TimestampedValue(element, element['timestamp']))
           | 'WindowedTeamScore'      >> WindowedTeamScore(3600000) # 1 hour = 3600 seconds = 3600000 milliseconds
           | 'FormatTeamScoreSums'    >> ParDo(FormatTeamScoreSumsFn())
           | 'WriteTeamScoreSums'     >> WriteToBigQuery(
                    args.output_table_name,
                    args.output_dataset,
                    options.get_all_options().get("project"),
                    table_schema()
            )
        )

if __name__ == "__main__":
    main(sys.argv)
