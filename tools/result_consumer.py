from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

# TODO(developer)
project_id = "text-analysis-323506"
subscription_id = "test_consumer"
# Number of seconds the subscriber should listen for messages
timeout = 15.0


class PubSubSubscriber(object):

    def __init__(self, project_id: str, subscription_id: str):
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(project_id, subscription_id)
        self.streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=PubSubSubscriber.callback)
        print(f"Listening for messages on {self.subscription_path}..\n")

    @staticmethod
    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message}.")
        message.ack()

    def retrieve(self):

        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with self.subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                self.streaming_pull_future.result(timeout=timeout)
            except TimeoutError:
                self.streaming_pull_future.cancel()  # Trigger the shutdown.
                self.streaming_pull_future.result()  # Block until the shutdown is complete.
