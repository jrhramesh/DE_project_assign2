"""Creates the 'flights' topic. Adapted from lab9/kafka-producer-game/kafkaadmin.py. Run on your laptop."""
from kafka.admin import KafkaAdminClient, NewTopic

if __name__ == '__main__':
    admin_client = KafkaAdminClient(bootstrap_servers="35.255.233.56:9092",   # your VM's external IP
                                    client_id='flights_admin')
    admin_client.create_topics(
        new_topics=[NewTopic(name="flights", num_partitions=1, replication_factor=1)],
        validate_only=False)
    print(admin_client.list_topics())
