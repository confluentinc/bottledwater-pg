require 'spec_helper'
require 'format_contexts'
require 'test_cluster'

describe 'error handling', functional: true, format: :json do
  after(:example) do
    TEST_CLUSTER.stop(dump_logs: false)
  end

  LONG_STRING = ('x' * 2_000_000).freeze
  LARGE_JSON = JSON.generate({}.tap do |h|
    100_000.times do |i|
      h["property#{i}"] = i
    end
  end).freeze

  let(:postgres) { TEST_CLUSTER.postgres }

  def fetch_json(object, name)
    JSON.parse(fetch_string(object, name))
  end

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

    example 'if existing data contains a large value, Bottled Water crashes during snapshot' do
      TEST_CLUSTER.before_service(TEST_CLUSTER.bottledwater_service, 'Prepopulating users table') do |cluster|
        cluster.postgres.exec('CREATE TABLE users (id SERIAL PRIMARY KEY, prefs JSONB)')
        cluster.postgres.exec_params(%{INSERT INTO users (prefs) VALUES ($1)}, ['{"colour":"red"}'])
        cluster.postgres.exec_params(%{INSERT INTO users (prefs) VALUES ($1)}, [LARGE_JSON])
        cluster.postgres.exec_params(%{INSERT INTO users (prefs) VALUES ($1)}, ['{"colour":"blue"}'])
      end

      expect { TEST_CLUSTER.start }.to raise_error(/bottledwater.* not ready/i)
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

    example 'if existing data contains a large value, Bottled Water skips that row during snapshot' do
      TEST_CLUSTER.before_service(TEST_CLUSTER.bottledwater_service, 'Prepopulating users table') do |cluster|
        cluster.postgres.exec('CREATE TABLE users (id SERIAL PRIMARY KEY, prefs JSONB)')
        cluster.postgres.exec_params(%{INSERT INTO users (prefs) VALUES ($1)}, ['{"colour":"red"}'])
        cluster.postgres.exec_params(%{INSERT INTO users (prefs) VALUES ($1)}, [LARGE_JSON])
        cluster.postgres.exec_params(%{INSERT INTO users (prefs) VALUES ($1)}, ['{"colour":"blue"}'])
      end

      TEST_CLUSTER.start

      messages = kafka_take_messages('users', 2)
      prefs = messages.map {|message| fetch_json(decode_value(message.value), 'prefs') }
      prefs.each {|pref| expect(pref).to have_key('colour') }
      colours = prefs.map {|pref| pref.fetch('colour') }
      expect(colours).to eq(['red', 'blue'])
    end
  end
end
