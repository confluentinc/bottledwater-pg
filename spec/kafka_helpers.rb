require 'kafka-consumer'
require 'timeout'

module KafkaHelpers
  def kafka_take_messages(topic, expected, wait: 5, collect_partitions: false)
    consumer = Kafka::Consumer.new(
      'test',
      [topic],
      zookeeper: TEST_CLUSTER.zookeeper_hostport,
      initial_offset: :earliest_offset,
      logger: logger)

    messages = []
    partitions = Hash.new do |h, k|
      h[k] = []
    end

    timeout(wait) do
      consumer.each do |message|
        messages << message
        partitions[message.partition] << message
        consumer.interrupt if messages.size >= expected
      end
    end

    if collect_partitions
      partitions
    else
      messages
    end
  rescue Timeout::Error
    problem = messages.empty? ? "didn't see any" : "only saw #{messages.size}"
    raise "expected #{expected} messages, but #{problem} after #{wait} seconds"
  ensure
    consumer.interrupt if consumer
  end
end

