require 'kafka-consumer'
require 'timeout'

module KafkaHelpers
  def kafka_take_messages(topic, expected, wait: 5)
    consumer = Kafka::Consumer.new(
      'test',
      [topic],
      zookeeper: TEST_CLUSTER.zookeeper_hostport,
      initial_offset: :earliest_offset,
      logger: logger)

    messages = []
    timeout(wait) do
      consumer.each do |message|
        messages << message
        consumer.interrupt if messages.size >= expected
      end
    end

    messages
  rescue Timeout::Error
    problem = messages.empty? ? "didn't see any" : "only saw #{messages.size}"
    raise "expected #{expected} messages, but #{problem} after #{wait} seconds"
  ensure
    consumer.interrupt
  end
end

