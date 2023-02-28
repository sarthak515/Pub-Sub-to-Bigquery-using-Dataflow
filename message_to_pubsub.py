from google.cloud import pubsub_v1
import time
import datetime as dt

# TODO(developer)
project_id = "mlconsole-poc"
topic_id = "sarthak11"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

for n in range(1, 10):
    data_str ='{"Name":"sarthak","Age":24,"City":"pune","Company":"psl","Salary":"471.900"}'
    
    # Data must be a bytestring
    data = data_str.encode("utf-8")
    time.sleep(1)
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data,origin="python-sample",username='gcp')
    #print(future.result())
    print(data)

print(f"Published messages to {topic_path}.")
