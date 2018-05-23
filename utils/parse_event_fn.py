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

#
# Parses the raw game event info into GameEvent objects. Each event line has the following
# format: username,teamname,score,timestamp_in_ms,readable_time,event_id
# e.g.:
# user2_AsparagusPig,AsparagusPig,10,1445230923951,
#     2015-11-02 09:09:28.224,e8018d7d-18a6-4265-ba7e-55666b898b6f
# The human-readable time string is not used here.
#

from apache_beam import DoFn
from apache_beam.metrics import Metrics

class ParseEventFn(DoFn):
    """Parses each line from the input CSV file into a dictionary"""

    def __init__(self):
        self.parsed_records = Metrics.counter('ParseGameEvent', 'ParsedRecords')
        self.parse_errors = Metrics.counter('ParseGameEvent', 'ParseErrors')

    def process(self, element):
        fields = element.split(',');
        try:
            yield {
                "user": fields[0].strip(),
                "team": fields[1].strip(),
                "score": long(fields[2]),
                "timestamp": long(fields[3]),
                "timestamp_string": fields[4],
                "event_id": fields[5]
            }
            self.parsed_records.inc()
        except:
            print "Error parsing record: " + element
            self.parse_errors.inc()