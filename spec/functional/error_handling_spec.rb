require 'spec_helper'
require 'format_contexts'
require 'test_cluster'

describe 'error handling', functional: true, format: :json do
  after(:example) do
    TEST_CLUSTER.stop(dump_logs: false)
  end

  LONG_STRING = ('x' * 2_000_000).freeze

  let(:postgres) { TEST_CLUSTER.postgres }

  describe 'with --on-error=exit' do
    before(:example) do
      TEST_CLUSTER.bottledwater_on_error = :exit
    end

    example 'BW crashes after publishing a message if Kafka is down' do
      TEST_CLUSTER.start(without: [:kafka])

      postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
      postgres.exec('INSERT INTO things (thing) VALUES (42)')
      sleep 5

      expect(TEST_CLUSTER.bottledwater_running?).to be_falsy
    end

    describe 'in Avro mode if the schema registry is down' do
      before(:example) do
        TEST_CLUSTER.bottledwater_format = :avro
        TEST_CLUSTER.start(without: [:'schema-registry'])

        postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
      end

      example 'initial message crashes Bottled Water' do
        postgres.exec('INSERT INTO things (thing) VALUES (42)')
        sleep 5

        expect(TEST_CLUSTER.bottledwater_running?).to be_falsy
      end
    end

    example 'writing a large value crashes Bottled Water' do
      TEST_CLUSTER.start

      postgres.exec('CREATE TABLE events (id SERIAL PRIMARY KEY, event TEXT)')
      postgres.exec_params('INSERT INTO events (event) VALUES ($1)', [LONG_STRING])
      sleep 5

      expect(TEST_CLUSTER.bottledwater_running?).to be_falsy
    end
  end

  describe 'with --on-error=log' do
    before(:example) do
      TEST_CLUSTER.bottledwater_on_error = :log
    end

    example 'BW does not crash if Kafka is down' do
      TEST_CLUSTER.start(without: [:kafka])

      postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
      postgres.exec('INSERT INTO things (thing) VALUES (42)')
      sleep 5

      expect(TEST_CLUSTER.bottledwater_running?).to be_truthy
    end

    describe 'in Avro mode if the schema registry is down' do
      before(:example) do
        TEST_CLUSTER.bottledwater_format = :avro
        TEST_CLUSTER.start(without: [:'schema-registry'])

        postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
      end

      example 'initial message does not crash Bottled Water' do
        postgres.exec('INSERT INTO things (thing) VALUES (42)')
        sleep 5

        expect(TEST_CLUSTER.bottledwater_running?).to be_truthy
      end

      example 'subsequent messages do not crash Bottled Water' do
        postgres.exec('INSERT INTO things (thing) VALUES (42)')
        sleep 1

        postgres.exec('INSERT INTO things (thing) VALUES (43)')
        sleep 5

        expect(TEST_CLUSTER.bottledwater_running?).to be_truthy
      end
    end

    example 'a row with a large value gets skipped without stopping replication' do
      TEST_CLUSTER.start

      postgres.exec('CREATE TABLE events (id SERIAL PRIMARY KEY NOT NULL, event TEXT)')
      postgres.exec_params('INSERT INTO events (event) VALUES ($1)', ['Wednesday'])
      postgres.exec_params('INSERT INTO events (event) VALUES ($1)', [LONG_STRING])
      postgres.exec_params('INSERT INTO events (event) VALUES ($1)', ['Friday'])
      sleep 1

      messages = kafka_take_messages('events', 2)
      events = messages.map {|message| fetch_string(decode_value(message.value), 'event') }
      expect(events).to eq(['Wednesday', 'Friday'])
    end
  end
end
