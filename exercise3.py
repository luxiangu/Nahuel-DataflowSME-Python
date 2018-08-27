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
import logging
import sys

import apache_beam as beam
from apache_beam.io import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam import ParDo, PTransform, DoFn, WindowInto
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema

from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.window import TimestampedValue, FixedWindows

from utils.change_me import ChangeMe
from utils.parse_event_fn import ParseEventFn

# Defines the schema for the BigQuery table where results will be stored
from utils.table_field import table_field

#
# Third in a series of coding exercises in a gaming domain.
#
# This is the same pipeline as in Exercise 2, but can run in either batch or streaming mode.
#
# See README.md for details.
#

def table_schema():
    """
    The schema of the table where we'll be writing the output data
    """
    table_schema = TableSchema()
    table_schema.fields = [
        table_field('team', 'STRING'),
        table_field('total_score', 'INTEGER'),
        table_field('window_start', 'TIMESTAMP')
    ]
    return table_schema

class ExtractAndSumScore(PTransform):
    """
    From a PCollection of game event structures, extracts the value of the given index and the event's score,
    then does a (per-window) per-key sum of the score
    """
    def __init__(self, field):
        super(PTransform, self)
        self.field = field

    def expand(self, game_events):
        return (game_events
                | beam.Map(lambda e: (e[self.field], e['score']))
                | beam.CombinePerKey(sum)
                )


class FormatTeamScoreSumsFn (DoFn):
    """
    Formats a team score as created by ExtractAndSumScore into a row to be sent to BigQuery
    We need to divide the window start by a different factor in streaming vs. batch to
    compensate for different types of timestamps being used.
    """

    def __init__(self, streaming):
        self.streaming = streaming

    def process(self, element, window=beam.DoFn.WindowParam):
        if self.streaming:
            scalefactor = 1000000
        else:
            scalefactor = 1000000000

        logging.info("Window.start: " + str(window.start))

        row = {
            'team': element[0],
            'total_score': element[1],
            'window_start': (window.start.micros / scalefactor)
        }
        yield row


class WindowedTeamScore (PTransform):
    """
    Groups data into windows and sums the total for each window for each team
    """
    def __init__(self, duration):
        self.duration = duration;

    def expand(self, input):
        return (input
                | WindowInto(FixedWindows(self.duration))
                | ExtractAndSumScore("team")
                )


class ReadGameEvents(PTransform):
    """
    Read the game events from either text files or Pub/Sub topic.
    """

    TIMESTAMP_ATTRIBUTE = "timestamp"

    def __init__(self, args):
        self.args = args

    def expand(self, input):
        # [START EXERCISE 3]:
        # Docs: https://beam.apache.org/documentation/sdks/pydoc/2.5.0/apache_beam.io.gcp.pubsub.html

        # Determine whether to use files or topic based on options.
        if (not self.args.input == None) and (not self.args.input == "") :
            return (input
                # Read game events from files. See exercise2.
                # Don't forget to parse events or to include the TimestampedValue transform to assign timestamps to events.
                | ChangeMe()
                | ChangeMe()
                | ChangeMe()
            )
        else:
            return (input
                    # Read game events from Pub/Sub topic self.options.topic using custom timestamps, which
                    # are extracted from the pubsub attribute TIMESTAMP_ATTRIBUTE.
                    # Use ReadFromPubSub() and use parameters topic and timestamp_attribute.
                    # https://beam.apache.org/documentation/sdks/python-streaming/
                    | ChangeMe()

                    # Parse the messages the same way as when they come from the text file. Note that we no
                    # longer have to run WithTimestamps transform, as the timestamps are already set by
                    # ReadFromPubSub.
                    | ChangeMe()
                    )
                    # [END EXERCISE 3]

#
# Run a batch or streaming pipeline.
 #
def main(argv):
    """Main entry point"""

    # Define and parse command line arguments
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        type=str,
                        default='',
                        help='Path to the data file(s) containing game data (use either this parameter or --topic but not both).')

    parser.add_argument('--topic',
                        type=str,
                        default='',
                        help='Topic to subscribe to (use either this parameter or --input but not both).')

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

    with beam.Pipeline(options=options) as p:
        (p | 'ReadGameEvents'         >> ReadGameEvents(args)
           | 'WindowedTeamScore'      >> WindowedTeamScore(30)
           | 'FormatTeamScoreSums'    >> ParDo(FormatTeamScoreSumsFn((args.topic != None) and (args.topic != "")))
           | 'WriteTeamScoreSums'     >> WriteToBigQuery(
                     args.output_table_name,
                     args.output_dataset,
                     options.get_all_options().get("project"),
                     table_schema(),
                     BigQueryDisposition.CREATE_IF_NEEDED,
                     BigQueryDisposition.WRITE_APPEND
                 )
           )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main(sys.argv)
