import argparse
import apache_beam as beam
import csv
import json
import numpy as np
from google.cloud import pubsub_v1
from google.oauth2 import service_account

# Define your Google Cloud project ID
project_id = ''

# Define the path to your service account key JSON file
credentials = service_account.Credentials.from_service_account_file('credentials.json')

def extract_recording_id(file_path):
    """Extracts recording ID from file path."""
    file_name = file_path.split('/')[-1]
    return file_name[:2]  # Assuming the first two characters represent the recording ID

def is_linear(array, tolerance=0.4):
    """
    Checks if the given array resembles a linear form.

    Args:
        array (list): The input array.
        tolerance (float, optional): Tolerance level for differences. Defaults to 0.01.

    Returns:
        bool: True if the array resembles a linear form, False otherwise.
    """

    # Calculate differences
    differences = [array[i + 1] - array[i] for i in range(len(array) - 1)]
    # Check linearity
    for i in range(len(differences)):
        if abs(differences[i]) < tolerance:
            return False

    return True

def compute_trajectory(records):
    composite_key, record_list = records
    first_record = record_list[0]
    track_id = first_record[1]
    x_centers = []
    y_centers = []
    headings = []
    width = first_record[7]
    height = first_record[8]
    xVelocity = []
    yVelocity = []
    xAcceleration = []
    yAcceleration = []
    lonVelocity = []
    latVelocity = []
    lonAcceleration = []
    latAcceleration = []

    for record in record_list:
        x_centers.append(float(record[4]))
        y_centers.append(float(record[5]))
        headings.append(float(record[6]))
        # Take absolute values to measure speed and acceleration
        xVelocity.append(abs(float(record[9])))
        yVelocity.append(abs(float(record[10])))
        xAcceleration.append(abs(float(record[11])))
        yAcceleration.append(abs(float(record[12])))
        lonVelocity.append(abs(float(record[13])))
        latVelocity.append(abs(float(record[14])))
        lonAcceleration.append(abs(float(record[15])))
        latAcceleration.append(abs(float(record[16])))

    # Compute the differences in heading between consecutive frames
    heading_diff = np.diff(headings)

    # Calculate the average heading difference
    avg_heading_diff = np.mean(heading_diff)

    # Calculate the average of other attributes
    avg_xVelocity = np.mean(xVelocity)
    avg_yVelocity = np.mean(yVelocity)
    avg_xAcceleration = np.mean(xAcceleration)
    avg_yAcceleration = np.mean(yAcceleration)
    avg_lonVelocity = np.mean(lonVelocity)
    avg_latVelocity = np.mean(latVelocity)
    avg_lonAcceleration = np.mean(lonAcceleration)
    avg_latAcceleration = np.mean(latAcceleration)

    # Check if the average heading difference is 0 -> T = Stationary object
    if avg_heading_diff == 0:
        trajectory = "Parked"
    else:
        # Check if x_center metric resembles a linear plot -> T = Straight trajectory
        if is_linear(x_centers):
            trajectory = "Straight"
        else:
            if avg_heading_diff < 0:
                # Check the direction of change in x_centers -> if x_center of last frame is greater than intial frame, classify as left
                dx = x_centers[-1] - x_centers[0]
                if dx > 0:
                    trajectory = "Left Turn"
                else:
                    trajectory = "Right Turn"
            else:
                trajectory = "Straight"

    # Prepare the message
    message = {
        'recordingId': first_record[0],
        'trackId': track_id,
        'width': width,
        'height': height,
        'trajectory': trajectory,
        'avgXVel': avg_xVelocity,
        'avgYVel': avg_yVelocity,
        'avgXAcc': avg_xAcceleration,
        'avgYAcc': avg_yAcceleration,
        'avgLonVel': avg_lonVelocity,
        'avgLatVel': avg_latVelocity,
        'avgLonAcc': avg_lonAcceleration,
        'avgLatAcc': avg_latAcceleration
    }

    return message

def run(input_bucket, output_table, temp_location):
    with beam.Pipeline() as pipeline:
        # Read the tracks.csv file
        tracks = (
            pipeline
            | 'List Files' >> beam.Create([f'{input_bucket}/{file}' for file, _ in beam.io.gcp.gcsio.GcsIO().list_files(input_bucket) if file.endswith('tracks.csv')])
            | 'Read Tracks' >> beam.io.ReadFromText(file_pattern=input_bucket + '*tracks.csv', skip_header_lines=1)
            | 'Parse Tracks CSV' >> beam.Map(lambda line: next(csv.reader([line])))
            | 'Add composite key' >> beam.Map(lambda record: ((record[0], record[1]), record))
            | 'Group by composite key' >> beam.GroupByKey()
            | 'Compute trajectory' >> beam.Map(compute_trajectory)
        )

        # Write trajectory results to BigQuery
        _ = tracks | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table=output_table,
            schema='recordingId:STRING, trackId:INTEGER, width:FLOAT, height:FLOAT, trajectory:STRING, avgXVel:FLOAT, avgYVel:FLOAT, avgXAcc:FLOAT, avgYAcc:FLOAT, avgLonVel:FLOAT, avgLatVel:FLOAT, avgLonAcc:FLOAT, avgLatAcc:FLOAT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location=temp_location  # Specify GCS temp location
        )

        # Publish messages to the vehicle_trajectory topic
        _ = tracks | 'Publish to Pub/Sub' >> beam.Map(
            lambda message: publish_message(message)
        )

def publish_message(message):
    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    if message['trajectory'] == 'Parked':
        topic_name = 'parked'
    elif message['trajectory'] == 'Straight':
        topic_name = 'straight'
    elif message['trajectory'] == 'Left Turn':
        topic_name = 'left'
    elif message['trajectory'] == 'Right Turn':
        topic_name = 'right'
    else:
        topic_name = 'unknown'  # Handle unexpected trajectory
        
    topic_path = publisher.topic_path(project_id, topic_name)
    data = json.dumps(message).encode('utf-8')
    future = publisher.publish(topic_path, data)
    print(f"Published message to {topic_name} topic: {message}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Dataflow pipeline script')
    parser.add_argument('--bucket', type=str, required=True, help='GCS bucket containing tracks.csv files')
    parser.add_argument('--output_table', type=str, required=True, help='BigQuery output table')
    parser.add_argument('--temp_location', type=str, required=True, help='GCS temporary location')
    args = parser.parse_args()
    run(args.bucket, args.output_table, args.temp_location)