import argparse
import json
from google.cloud import pubsub_v1
from google.oauth2 import service_account

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received message: {message.data}.")
    message.ack()

def receive_messages(subscription_id):
    project_id = ''
    credentials = service_account.Credentials.from_service_account_file('credentials.json')
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    with subscriber:
        streaming_pull_future.result()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Pub/Sub message consumer')
    parser.add_argument('--subscription_id', type=str, required=True, help='Pub/Sub subscription ID')
    args = parser.parse_args()

    receive_messages(args.subscription_id)