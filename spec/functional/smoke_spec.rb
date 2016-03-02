require 'spec_helper'

require 'kafka-consumer'
require 'timeout'

describe 'smoke test', functional: true do
  before(:context) do
    require 'test_cluster'
    TEST_CLUSTER.start
  end

  after(:context) do
    TEST_CLUSTER.stop
  end

  let(:postgres) { TEST_CLUSTER.postgres }
  let(:kafka) do
    Kafka::Consumer.new(
      'test',
      ['things'],
      zookeeper: TEST_CLUSTER.zookeeper_hostport,
      initial_offset: :earliest_offset,
      logger: logger)
  end

  after(:example) do
    kafka.interrupt
  end

  example 'inserting rows in Postgres should publish messages to Kafka' do
    postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
    postgres.exec('INSERT INTO things (thing) SELECT * FROM generate_series(1, 10) AS thing')
    sleep 1

    messages = []
    timeout(5) do
      kafka.each do |message|
        messages << message
        kafka.interrupt if messages.size >= 10
      end
    end

    expect(messages.size).to eq 10
  end
end
