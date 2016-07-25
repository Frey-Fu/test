from confluent_kafka import Producer
import sys

if __name__ == '__main__':
    if len(sys.argv) != 4:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic> <logfilename>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]
    log = sys.argv[3]

    conf = {'bootstrap.servers': broker}

    p = Producer(**conf)

    def delivery_callback (err, msg):
        if err:
            sys.stderr.write('%% Message failer delivery: %s\n' %err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d]\n' % \
                             (msg.topic(), msg.partition()))

    # Read lines from stdin, produce each line to Kafka
    f = open(log, 'r')
    for line in f:
        try:
            # Produce line (without newline)
            tmp = line.partition(' - - ')
            ip = tmp[0]
            p.produce(topic, ip.rstrip(), callback=delivery_callback)

        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full ' \
                             '(%d messages awaiting delivery): try again\n' %
                             len(p))

        # Serve delivery callback queue.
        # NOTE: Since produce() is an asynchronous API this poll() call
        #       will most likely not serve the delivery callback for the
        #       last produce()d message.
        p.poll(0)

    close(f)
    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()