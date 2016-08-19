require 'spec_helper'
require 'format_contexts'
require 'test_cluster'

shared_examples 'publishing messages' do |format, postgres_version, valgrind|
  # We only stop the cluster after all examples in the context have run, so
  # state in Postgres, Kafka and Bottled Water can leak between examples.  We
  # therefore need to make sure examples look at different tables, so they
  # don't affect each other.

  let(:postgres) { TEST_CLUSTER.postgres }

  describe 'table with a primary key' do
    before(:context) do
      TEST_CLUSTER.bottledwater_format = format
      TEST_CLUSTER.postgres_version = postgres_version
      TEST_CLUSTER.valgrind = valgrind

      TEST_CLUSTER.before_service(TEST_CLUSTER.bottledwater_service, 'Prepopulating users table') do |cluster|
        cluster.postgres.exec('CREATE TABLE users (id SERIAL PRIMARY KEY, username TEXT)')
        cluster.postgres.exec(%{INSERT INTO users (username) SELECT 'user' || num FROM generate_series(1, 10) AS num})
      end

      TEST_CLUSTER.start
    end

    after(:context) do
      TEST_CLUSTER.stop
    end

    example 'inserting rows in Postgres should publish the rows to Kafka in order' do
      postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
      postgres.exec('INSERT INTO things (thing) SELECT * FROM generate_series(1, 10) AS thing')
      sleep 1

      messages = kafka_take_messages('things', 10)

      expect(messages.size).to eq 10

      messages.each_with_index do |message, index|
        key = decode_key message.key
        value = decode_value message.value

        expect(fetch_int(key, 'id')).to eq(index + 1)

        expect(fetch_int(value, 'thing')).to eq(index + 1)
      end
    end

    example 'deleting a row in Postgres should publish a null message to Kafka' do
      postgres.exec('CREATE TABLE widgets (id SERIAL PRIMARY KEY, widget TEXT)')
      postgres.exec_params('INSERT INTO widgets (widget) VALUES ($1)', ['Hello'])
      postgres.exec('DELETE FROM widgets'); # no WHERE, don't try this at home
      sleep 1

      messages = kafka_take_messages('widgets', 2)

      expect(messages[1].key).to eq(messages[0].key)
      expect(messages[0].value).to match(/Hello/)
      expect(messages[1].value).to be_nil
    end

    example 'updating a row in Postgres should publish the new value to Kafka' do
      postgres.exec('CREATE TABLE gadgets (id SERIAL PRIMARY KEY, gadget TEXT)')
      postgres.exec_params('INSERT INTO gadgets (gadget) VALUES ($1)', ['Hello'])
      postgres.exec_params('UPDATE gadgets SET gadget = $1', ['Goodbye']); # no WHERE, don't try this at home
      sleep 1

      messages = kafka_take_messages('gadgets', 2)

      expect(messages[1].key).to eq(messages[0].key)
      expect(messages[0].value).to match(/Hello/)

      new_value = decode_value(messages[1].value)
      expect(fetch_string(new_value, 'gadget')).to eq('Goodbye')
    end

    describe 'initial database snapshot' do
      # uses the table that was prepopulated in the before hook above

      example 'publishes the existing database contents into Kafka, then streams ongoing updates' do
        messages = kafka_take_messages('users', 10)

        messages.each do |message|
          value = decode_value message.value
          expect(fetch_string(value, 'username')).to match(/^user\d+/)
        end

        postgres.exec(%{INSERT INTO users (username) VALUES('user11')})

        message_after_snapshot = kafka_take_messages('users', 1).first

        value = decode_value message_after_snapshot.value
        expect(fetch_string(value, 'username')).to eq('user11')
      end
    end
  end

  describe 'unkeyed table' do
    before(:context) do
      TEST_CLUSTER.bottledwater_format = format
      TEST_CLUSTER.postgres_version = postgres_version
      TEST_CLUSTER.valgrind = valgrind

      # Kafka 0.9 rejects unkeyed messages sent to a compacted table, but we
      # set compaction as default in test_cluster.rb, so we need to explicitly
      # disable it for these topics.
      TEST_CLUSTER.kafka_log_cleanup_policy = :delete

      TEST_CLUSTER.start
    end

    after(:context) do
      TEST_CLUSTER.stop
    end

    example 'inserting rows should publish unkeyed messages' do
      postgres.exec('CREATE TABLE logs (message TEXT NOT NULL)')
      postgres.exec_params('INSERT INTO logs (message) VALUES ($1)', ['Launching missiles'])
      sleep 1

      messages = kafka_take_messages('logs', 1)

      expect(messages[0].key).to be_nil
      value = decode_value(messages[0].value)
      expect(fetch_string(value, 'message')).to eq('Launching missiles')
    end

    example 'deleting rows should not publish any messages' do
      # hard to verify that a message *wasn't* received, so we rely on the
      # ordering of messages.

      postgres.exec('CREATE TABLE events (details TEXT NOT NULL)')
      postgres.exec_params('INSERT INTO events (details) VALUES ($1)', ['User 1 signup'])
      postgres.exec('DELETE FROM events'); # no WHERE, don't try this at home
      postgres.exec_params('INSERT INTO events(details) VALUES($1)', ['User 2 signup'])
      sleep 1

      messages = kafka_take_messages('events', 2) # expecting no message for the DELETE

      message_details = messages.map do |message|
        expect(message.value).not_to be_nil
        fetch_string(decode_value(message.value), 'details')
      end

      expect(message_details).to eq(['User 1 signup', 'User 2 signup'])
    end

    example 'updating rows should publish the new value' do
      postgres.exec('CREATE TABLE numbers (number INTEGER NOT NULL)')
      postgres.exec_params('INSERT INTO numbers (number) VALUES ($1)', [42])
      postgres.exec_params('UPDATE numbers SET number = $1', [43]) # no WHERE, don't try this at home
      sleep 1

      messages = kafka_take_messages('numbers', 2)

      numbers = messages.map do |message|
        expect(message.value).not_to be_nil
        fetch_int(decode_value(message.value), 'number')
      end
      expect(numbers).to eq([42, 43])
    end
  end
end


describe 'publishing messages (JSON, Postgres 9.4)', functional: true, format: :json, postgres: '9.4' do
  include_examples 'publishing messages', :json, '9.4', false
end

describe 'publishing messages (Avro, Postgres 9.4)', functional: true, format: :avro, postgres: '9.4' do
  include_examples 'publishing messages', :avro, '9.4', false
end

describe 'publishing messages (JSON, Postgres 9.5)', functional: true, format: :json, postgres: '9.5' do
  include_examples 'publishing messages', :json, '9.5', false
end

describe 'publishing messages (Avro, Postgres 9.5)', functional: true, format: :avro, postgres: '9.5' do
  include_examples 'publishing messages', :avro, '9.5', false
end

describe 'publishing messages (JSON, Valgrind)', functional: true, format: :json, postgres: '9.5', valgrind: true do
  include_examples 'publishing messages', :json, '9.5', true
end

describe 'publishing messages (Avro, Valgrind)', functional: true, format: :avro, postgres: '9.5', valgrind: true do
  include_examples 'publishing messages', :avro, '9.5', true
end
