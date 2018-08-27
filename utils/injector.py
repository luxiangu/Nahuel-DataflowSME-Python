import random

import time
import uuid

import sys

import datetime
from google.cloud import pubsub

topic = None
play_topic = None

timestamp_attribute = "timestamp"
message_id_attribute = "unique_id"

# QPS ranges from 500 to 700 for score events.
MIN_QPS = 500
QPS_RANGE = 200
# How long to sleep, in ms, between creation of the threads that make API requests to PubSub.
THREAD_SLEEP_MS = 500

# Lists used to generate random team names.
COLORS = [  "Magenta",
            "AliceBlue",
            "Almond",
            "Amaranth",
            "Amber",
            "Amethyst",
            "AndroidGreen",
            "AntiqueBrass",
            "Fuchsia",
            "Ruby",
            "AppleGreen",
            "Apricot",
            "Aqua",
            "ArmyGreen",
            "Asparagus",
            "Auburn",
            "Azure",
            "Banana",
            "Beige",
            "Bisque",
            "BarnRed",
            "BattleshipGrey"]

ANIMALS = [ "Echidna",
            "Koala",
            "Wombat",
            "Marmot",
            "Quokka",
            "Kangaroo",
            "Dingo",
            "Numbat",
            "Emu",
            "Wallaby",
            "CaneToad",
            "Bilby",
            "Possum",
            "Cassowary",
            "Kookaburra",
            "Platypus",
            "Bandicoot",
            "Cockatoo",
            "Antechinus"]


# The list of live teams.
live_teams = []

timestamp_format = "%Y-%m-%d %H:%M:%S.000"

# The total number of robots in the system.
NUM_ROBOTS = 20;
# Determines the chance that a team will have a robot team member.
ROBOT_PROBABILITY = 3;
NUM_LIVE_TEAMS = 15;
BASE_MEMBERS_PER_TEAM = 5;
MEMBERS_PER_TEAM = 15;
MAX_SCORE = 20;
LATE_DATA_RATE = 5 * 60 * 2; # Every 10 minutes
BASE_DELAY_IN_MILLIS = 5 * 60 * 1000; # 5-10 minute delay
FUZZY_DELAY_IN_MILLIS = 5 * 60 * 1000;

# The minimum time a 'team' can live.
BASE_TEAM_EXPIRATION_TIME_IN_MINS = 20;
TEAM_EXPIRATION_TIME_IN_MINS = 20;


def currentTimeInMillis():
  return int(round(time.time() * 1000))

#
# A class for holding team info: the name of the team, when it started, and the current team
# members. Teams may but need not include one robot team member.
#
class TeamInfo:
  def __init__ (self, team_name, start_time_in_millis, robot):
    self.team_name = team_name
    self.start_time_in_millis = start_time_in_millis
    # How long until this team is dissolved.
    self.expiration_period = random.randrange(TEAM_EXPIRATION_TIME_IN_MINS) + BASE_TEAM_EXPIRATION_TIME_IN_MINS
    self.robot = robot
    # Determine the number of team members.
    self.num_members = random.randrange(MEMBERS_PER_TEAM) + BASE_MEMBERS_PER_TEAM

  def end_time_in_millis(self):
    return self.start_time_in_millis + (self.expiration_period * 60 * 1000)

  def get_random_user(self):
    userNum = random.randrange(self.num_members)
    return "user" + str(userNum) + "_" + self.team_name

  def __str__(self):
    return ("("
            + self.team_name
            + ", num members: "
            + str(self.num_members)
            + ", starting at: "
            + str(self.start_time_in_millis)
            + ", expires in: "
            + str(self.expiration_period)
            + ", robot: "
            + (self.robot or "No")
            + ")")

# Returns a random element from a list
def randomElement(list):
  return random.choice(list)

#
# Get and return a random team. If the selected team is too old w.r.t its expiration, remove it,
# replacing it with a new team.
#

def randomTeam(teams_list):
  team = random.choice(teams_list)
  # If the selected team is expired, remove it and return a new team.
  if (team.end_time_in_millis() < currentTimeInMillis()) or (team.num_members == 0):
    sys.stdout.write("team " + team.team_name + " is too old; replacing.\n")
    teams_list.remove(team)
    return addLiveTeam(teams_list);
  else:
    return team

#
# Create and add a team. Possibly add a robot to the team.
#
def addLiveTeam(teams_list):
  team_name = randomElement(COLORS) + randomElement(ANIMALS)
  robot = None
  # Decide if we want to add a robot to the team.
  if (random.randrange(ROBOT_PROBABILITY) == 0):
    robot = "Robot-" + str(random.randrange(NUM_ROBOTS))

  currTime = int(round(time.time() * 1000))

  # Create the new team.
  new_team = TeamInfo(team_name, currentTimeInMillis(), robot)
  live_teams.append(new_team)
  sys.stdout.write("[+" + str(new_team) + "]\n")
  return new_team

#
# Generate a score event.
#
def generateScoreEvent(current_time, delay_in_millis, id):
  team = randomTeam(live_teams)
  team_name = team.team_name
  parse_error_rate = 900000

  robot = team.robot

  # If the team has an associated robot team member...
  user = ""
  if (robot != None):
    # Then use that robot for the message with some probability.
    # Set this probability to higher than that used to select any of the 'regular' team
    # members, so that if there is a robot on the team, it has a higher click rate.
    if (random.randrange(team.num_members / 2) == 0):
      user = robot
    else:
      user = team.get_random_user()
  else:
    user = team.get_random_user()

  event_time = current_time - delay_in_millis

  # Randomly introduce occasional parse errors. You can see a custom counter tracking the number
  # of such errors in the Dataflow Monitoring UI, as the example pipeline runs.
  if (random.randrange(parse_error_rate) == 0):
    sys.stdout.write("Introducing a parse error.\n");
    event = "THIS LINE REPRESENTS CORRUPT DATA AND WILL CAUSE A PARSE ERROR"
  else:
    event = (user + ","
             + team_name + ","
             + str(random.randrange(MAX_SCORE)) + ", "
             + str(event_time) + ", "
             + datetime.datetime.fromtimestamp(event_time/ 1000).strftime(timestamp_format) + ","
             + str(id))

  return (user, event)


def generatePlayEvent(user, currrent_time, delay_in_millis, id):
  if (user.startsWith("Robot")):
    # This is a robot event, so generate a play before score with low delay (< 200 ms).
    play_milliseconds = random.randrange(200);
  else:
    # This is a real user event, so generate a play with a reasonable human delay (up to 10 sec).
    play_milliseconds = random.randrange(10000) + 500;

  play_event = (user + ","
                + str(currrent_time - play_milliseconds) + ","
                + str(delay_in_millis) + ","
                + id)
  return play_event



#
# Publish 'numMessages' arbitrary events from live users with the provided delay, to a PubSub
# topic.
#
def publishData(num_messages, delay_in_millis):
  global topic
  global play_topic
  for message in range(num_messages):
    current_time = currentTimeInMillis()
    id = uuid.uuid4()

    sys.stdout.write(".")
    sys.stdout.flush()
    if topic != None:
      user, message = generateScoreEvent(current_time, delay_in_millis, id);
      #print message
      topic.publish(message,
                          timestamp = str((current_time - delay_in_millis)),
                          unique_id = (str(id) + "_event"))

      if delay_in_millis != 0:
        sys.stdout.write("late data for: " + message + "n")

      if play_topic != None:
        for j in range (1, 5):
          play_message = generatePlayEvent(user, current_time, delay_in_millis, id)
          play_topic.publish(play_message,
                                   timestamp = str((current_time - delay_in_millis)),
                                   unique_id = str(id) + "_play_" + str(j))

          if delay_in_millis != 0:
            sys.stdout.write("late data for " + play_message)


# Main function
def main(args) :
  global publisher
  global topic
  global play_topic

  if len(args) < 4:
    print("Usage: Injector project-name (topic-name|none) (play-topic-name|none)\n")
    return
  project = args[1]
  topic_name = args[2]
  play_topic_name = args[3]

  print("Starting injector on project %s" % project)

  pubsub_client = pubsub.Client(project=project)

  if topic_name.lower() == "none":
    topic = None
  else:
    topic = pubsub_client.topic(topic_name)
    print("Injecting to topic: " + topic_name)

  if play_topic_name.lower() == "none":
    play_topic = None
  else:
    play_topic = pubsub_client.topic(play_topic_name)
    print("Injecting to play topic: " + play_topic_name)


  print("Starting Injector")

  # Start off with some random live teams.
  while (len(live_teams) < NUM_LIVE_TEAMS):
    addLiveTeam(live_teams)

  # Publish messages at a rate determined by the QPS and Thread sleep settings.
  period = 1 / MIN_QPS

  message_number = 0
  while True:
    message_number = message_number + 1

    if message_number % LATE_DATA_RATE == 0:
      delay_in_millis = (BASE_DELAY_IN_MILLIS
                        + random.randrange(FUZZY_DELAY_IN_MILLIS))
      num_messages = 1
      sys.stdout.write("DELAY(" + str(delay_in_millis) + ", " + str(num_messages) + ")")
    else:
      sys.stdout.write(".")
      delay_in_millis = 0
      num_messages = MIN_QPS + random.randrange(QPS_RANGE)


    publishData(num_messages, delay_in_millis)


if __name__ == "__main__":
  main(sys.argv)
