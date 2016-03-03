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
