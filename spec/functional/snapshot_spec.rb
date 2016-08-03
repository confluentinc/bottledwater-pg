require 'spec_helper'
require 'format_contexts'
require 'test_cluster'

describe 'initial database snapshot', functional: true, format: :json do
  let(:postgres) { TEST_CLUSTER.postgres }

  # We need to give each example a fresh cluster, since we're testing the
  # startup behaviour so it's inevitably stateful.
  before(:example) do
    TEST_CLUSTER.before_service(TEST_CLUSTER.bottledwater_service, 'Prepopulating users table') do
      postgres.exec('CREATE TABLE users (id SERIAL PRIMARY KEY, username TEXT)')
      postgres.exec(%{INSERT INTO users (username) SELECT 'user' || num FROM generate_series(1, 10) AS num})
    end
  end

  after(:example) do
    TEST_CLUSTER.stop
  end

  describe 'by default' do
    before(:example) do
      TEST_CLUSTER.start
    end

    example 'publishes the existing database contents into Kafka' do
      messages = kafka_take_messages('users', 10)

      messages.each do |message|
        value = decode_value message.value
        expect(fetch_string(value, 'username')).to match(/^user\d+/)
      end
    end

    example 'publishes ongoing inserts into Kafka' do
      postgres.exec(%{INSERT INTO users (username) VALUES('user11')})

      messages = kafka_take_messages('users', 11)
      message_after_snapshot = messages.last

      value = decode_value message_after_snapshot.value
      expect(fetch_string(value, 'username')).to eq('user11')
    end
  end

  describe 'with --skip-snapshot' do
    before(:example) do
      TEST_CLUSTER.bottledwater_skip_snapshot = true
      TEST_CLUSTER.start
    end

    example 'ignores the existing database contents' do
      postgres.exec(%{INSERT INTO users (username) VALUES('user11')})
      sleep 1

      message = kafka_take_messages('users', 1).first

      value = decode_value message.value
      expect(fetch_string(value, 'username')).to eq('user11')
    end
  end
end
