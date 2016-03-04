require 'spec_helper'

describe 'publishing messages', functional: true do
  # We only stop the cluster after all examples in the context have run, so
  # state in Postgres, Kafka and Bottled Water can leak between examples.  We
  # therefore need to make sure examples look at different tables, so they
  # don't affect each other.

  before(:context) do
    require 'test_cluster'
    TEST_CLUSTER.start
  end

  after(:context) do
    TEST_CLUSTER.stop
  end

  let(:postgres) { TEST_CLUSTER.postgres }

  describe 'table with a primary key' do
    example 'inserting rows in Postgres should publish the rows to Kafka in order' do
      postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
      postgres.exec('INSERT INTO things (thing) SELECT * FROM generate_series(1, 10) AS thing')
      sleep 1

      messages = kafka_take_messages('things', 10)

      expect(messages.size).to eq 10

      messages.each_with_index do |message, index|
        key = JSON.parse message.key
        value = JSON.parse message.value

        expect(key.fetch('id').fetch('int')).to eq(index + 1)

        expect(value.fetch('thing').fetch('int')).to eq(index + 1)
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

      new_value = JSON.parse(messages[1].value)
      expect(new_value.fetch('gadget').fetch('string')).to eq('Goodbye')
    end
  end

  describe 'unkeyed table' do
    example 'inserting rows should publish unkeyed messages' do
      postgres.exec('CREATE TABLE logs (message TEXT NOT NULL)')
      postgres.exec_params('INSERT INTO logs (message) VALUES ($1)', ['Launching missiles'])
      sleep 1

      messages = kafka_take_messages('logs', 1)

      expect(messages[0].key).to be_nil
      value = JSON.parse(messages[0].value)
      expect(value.fetch('message').fetch('string')).to eq('Launching missiles')
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
        JSON.parse(message.value).fetch('details').fetch('string')
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
        JSON.parse(message.value).fetch('number').fetch('int')
      end
      expect(numbers).to eq([42, 43])
    end
  end
end