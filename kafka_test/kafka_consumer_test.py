from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

if __name__ == '__main__':
    if len(sys.argv) < 4:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <group> <topic1> <topic2> ..\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    group = sys.argv[2]
    topics = sys.argv[3:]

    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'}}

    # Create Consumer instance
    c = Consumer(**conf)


    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)


    # Subscribe to topics
    c.subscribe(topics, on_assign=print_assignment)

    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    # Error
                    raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write('%% %s [%d] at offset %d with key %s\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key()), ))
                print(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

        # Close down consumer to commit final offsets.
c.close()