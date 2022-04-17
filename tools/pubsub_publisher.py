import json
from typing import Dict

from google.cloud import pubsub_v1


class PubSubPublisher(object):

    def __init__(self, project_id: str, topic_id: str, ):
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)

    def publish(self, message: Dict):

        # Convert dictionary to a json string
        message = json.dumps(message)

        # Data must be a byte string
        data = message.encode("utf-8")
        # When you publish a message, the client returns a future.
        _ = self.publisher.publish(self.topic_path, data)
