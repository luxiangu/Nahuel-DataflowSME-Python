# Tutorial for Cloud Dataflow

This is a collection of tutorial-style Dataflow exercises based on the [Dataflow
gaming
example](https://github.com/GoogleCloudPlatform/DataflowJavaSDK-examples/blob/master/src/main/java8/com/google/cloud/dataflow/examples/complete/game/README.md)
and inspired by the [Beam tutorial](https://github.com/eljefe6a/beamexample).

In the gaming scenario, many users play a fictional game, as members of
different teams, over the course of a day, and their events are logged for
processing.

The exercises either read the batch data from CSV files on GCS or the streaming
data from a [PubSub](https://cloud.google.com/pubsub/) topic (generated by the
included `Injector` program). All exercises write their output to BigQuery.

## Set up your environment

### Tools

1.  [Download and install Git](https://git-scm.com/downloads).
1.  Install Python 2.7. If you use Linux, install it through apt or yum. Otherwise [download it from the website](https://www.python.org/downloads/release/python-2715/).
1.  Install [virtualenv](https://virtualenv.pypa.io/en/stable/).
1.  Install Google [Cloud SDK](https://cloud.google.com/sdk/).
1.  Install an IDE that supports Python (optional).

### Set up a project

1.  Go to [https://cloud.google.com/console](https://cloud.google.com/console).
1.  Enable billing and create a project. For this project:
    1.  Enable Google Dataflow API and BigQuery API.
    1.  Create a GCS bucket and two folders inside the bucket:
        1. A staging folder.
        1. A temp folder.
    1.  Create a BigQuery dataset to store the results.

#### Prepare your environment

1. Create your SME environment. Open a shell (terminal or console) and cd into your home folder. 
There, run:

    ```shell
    $ virtualenv dataflowsme
    $ source dataflowsme/bin/activate
    (dataflowsme) $
    ```

1. Authenticate to Google Cloud using the gcloud command and set the default credentials and 
default project. You will need to replace YOUR-PROJECT-ID with the id of the project 
you created before:

    ```shell
    $ gcloud auth login
    $ gcloud auth application-default login
    $ gcloud config set project YOUR-PROJECT-ID
    ```
    
1. Get the project name with Gcloud and set it as an env variable:
    ```shell
    $ export PROJECT=`gcloud config get-value project`
    ```
    
1. Set other environment variables
    ```shell
    $ export STAGING_FOLDER=gs://<path of the bucket and staging folder that you created before>
    $ export TEMP_FOLDER=gs://<path of the bucket and temp folder that you created before>
    $ export BIGQUERY_DATASET=<name of the dataset that you created before>
    $ export USER=`whoami`
    ```

### Download the code

Clone the github repository

   ```shell
   $ git clone https://github.com/nahuellofeudo/DataflowSME-Python.git
   $ cd DataflowSME-Python
   ```

## Exercise 0 (prework)

**Goal**: Use the provided Dataflow pipeline to import the input events from a file in GCS to
BigQuery and run simple queries on the result.

**Procedure**:

1.  Compile and run the pipeline:

    ```shell
    $ python2.7 exercise0.py \
        --project=$PROJECT \
        --setup_file=./setup.py \
        --input=gs://dataflow-samples/game/gaming_data1.csv \
        --output_dataset=$BIGQUERY_DATASET \
        --output_table_name=events \
        --runner=DataflowRunner \
        --temp_location=$TEMP_FOLDER \
        --staging_location=$STAGING_FOLDER 
    ```

1.  Open [https://console.cloud.google.com](https://console.cloud.google.com) and navigate to the Dataflow UI.

1.  Once the pipeline finishes (should take about 15-20 minutes), the Job Status
    on the UI changes to Succeeded.

1.  After the pipeline finishes, check the value of `ParseGameEvent/ParseErrors`
    aggregator on the UI. Scroll down in the Summary tab to find it. 

1.  Check the number of distinct teams in the created BigQuery table.

    ```shell
    $ bq query --project_id=$PROJECT \
        "select count(distinct team) from $BIGQUERY_DATASET.events;"
    ```

## Exercise 1

**Goal**: Use Dataflow to calculate per-user scores and write them to BigQuery. 

**Procedure**

1.  Modify `exercise1.py`

1.  Run the pipeline (using Direct runner):

    ```shell
    $ python2.7 exercise1.py \
               --project=$PROJECT \
               --setup_file=./setup.py \
               --input=gs://dataflow-sme-tutorial/gaming_data0.csv \
               --output_dataset=$BIGQUERY_DATASET \
               --output_table_name=user_scores \
               --runner=DirectRunner \
               --temp_location=$TEMP_FOLDER \
               --staging_location=$STAGING_FOLDER 
    ```

1.  Once the pipeline finishes successfully check the score for
    'user0_AmberDingo':

    ```shell
    $ bq query --project_id=$PROJECT \
        "select total_score from $BIGQUERY_DATASET.user_scores \
         where user = \"user0_AmberDingo\";"
    ```

1.  Rerun the pipeline on the Dataflow service, but remove the BigQuery table
    first:

    ```shell
    $ bq rm --project_id=$PROJECT $BIGQUERY_DATASET.user_scores
    ```

    and then execute the above `mvn` command with

    ```shell
        --runner=DataflowRunner
    ```

## Exercise 2

**Goal**: Use Dataflow to calculate per-hour team scores and write them to BigQuery.

**Procedure**: 

1.  Modify `exercise2.py`

1.  Run the pipeline:

    ```shell
    $ python2.7 exercise2.py \
                     --project=$PROJECT \
                     --setup_file=./setup.py \
                     --input=gs://dataflow-sme-tutorial/gaming_data0.csv \
                     --output_dataset=$BIGQUERY_DATASET \
                     --output_table_name=hourly_team_scores \
                     --runner=DataflowRunner \
                     --temp_location=$TEMP_FOLDER \
                     --staging_location=$STAGING_FOLDER
    ```

1.  Once the pipeline finishes successfully check the score for team
    'AmberDingo':

    ```shell
    $ bq query --project_id=$PROJECT \
        "select total_score from $BIGQUERY_DATASET.hourly_team_scores \
         where team = \"AmberDingo\" and window_start = \"2017-03-18 16:00:00 UTC\";"
    ```

## Exercise 3

**Goal**: Convert the previous pipeline to run in streaming mode.

First, you need to set up the injector to publish scores via PubSub.

1.  Create and download a JSON key for Google Application Credentials. See
    [instructions](https://cloud.google.com/docs/authentication/getting-started).
    Make sure that the key's account has at least the following role:
    * Pub/Sub --> Editor
    
1.  Open a second terminal window. In this terminal run the commands listed 
in steps 2, 3 and 4 of the section "Prepare your enviroment" to set the same variables 
as in the first terminal (you do **not** need to do step 1).

1.  In the new terminal set the new credentials by running:

    ```shell
    $ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/credentials-key.json
    ```

1. Create a new topic:

    ```shell
    $ gcloud pubsub topics create game_events_$USER
    ```

1.  In the **second** terminal run the injector:

    ```shell
    $ python2.7 utils/injector.py $PROJECT game_events_$USER none
    ```

Now complete the exercise so that it runs the pipeline from Exercise 2 in either
batch or streaming mode.

**Procedure**:

1.  Modify `exercise3.py`

1.  Run the pipeline in batch mode:

    ```shell
    $ python2.7 exercise3.py \
                        --project=$PROJECT \
                        --setup_file=./setup.py \
                        --input=gs://dataflow-sme-tutorial/gaming_data0.csv \
                        --output_dataset=$BIGQUERY_DATASET \
                        --output_table_name=streaming_team_scores \
                        --runner=DataflowRunner \
                        --temp_location=$TEMP_FOLDER \
                        --staging_location=$STAGING_FOLDER
    ```

1.  Once the pipeline finishes successfully check the score for team
    'AmberDingo':

    ```shell
    $ bq query --project_id=$PROJECT \
        "select window_start, total_score from $BIGQUERY_DATASET.streaming_team_scores \
              where team = \"AmberDingo\";""
    ```
 
1. Delete the table so that the streaming job can create a new one:
    ```shell
    $ bq rm streaming_team_scores
    ```

1.  Run the pipeline in streaming mode (make sure that the injector is still running first!):

    ```shell
    $ python2.7 exercise3.py \
                        --project=$PROJECT \
                        --setup_file=./setup.py \
                        --topic=projects/$PROJECT/topics/game_events_$USER \
                        --output_dataset=$BIGQUERY_DATASET \
                        --output_table_name=streaming_team_scores \
                        --runner=DataflowRunner \
                        --temp_location=$TEMP_FOLDER \
                        --staging_location=$STAGING_FOLDER \
                        --streaming
    ```
 
 1. Once the pipeline starts, let it run for approximately 5 to 10 minutes. Then stop (cancel) the job.
 
 1. Check the new scores. 
 Since teams and windows are dynamically generated we can't just query for a single team so we query the whole history:
 
    ```shell
    $ bq query --project_id=$PROJECT \
        "select team, window_start, total_score from $BIGQUERY_DATASET.streaming_team_scores \
         order by window_start desc;"
    ```
