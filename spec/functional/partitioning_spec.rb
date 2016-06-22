require 'spec_helper'
require 'format_contexts'

describe 'partitioning', functional: true, format: :json do
  before(:context) do
    require 'test_cluster'

    # Some of these tests create tables without a primary key.  Kafka 0.9
    # rejects unkeyed messages sent to a compacted table, but we set
    # compaction as default in test_cluster.rb, so we need to explicitly
    # disable it for these tests.
    TEST_CLUSTER.kafka_log_cleanup_policy = :delete

    TEST_CLUSTER.start
  end

  after(:context) do
    TEST_CLUSTER.stop
  end

  let(:postgres) { TEST_CLUSTER.postgres }
  let(:kazoo) { TEST_CLUSTER.kazoo }

  describe 'keyed table' do
    example 'messages are partitioned evenly' do
      kazoo.create_topic('items', partitions: 2, replication_factor: 1)

      postgres.exec('CREATE TABLE items (id SERIAL PRIMARY KEY, item INTEGER NOT NULL)')

      postgres.exec('INSERT INTO items (item) SELECT * FROM generate_series(1, 100) AS item')
      sleep 1

      partitions = kafka_take_messages('items', 100, collect_partitions: true)
      expect(partitions.size).to eq(2)
      messages_0, messages_1 = partitions.values

      expect(messages_0.size).to be_within(10).of(messages_1.size)
    end

    example 'inserts and deletes for a given key go to the same partition' do
      kazoo.create_topic('numbers', partitions: 2, replication_factor: 1)

      postgres.exec('CREATE TABLE numbers (id SERIAL PRIMARY KEY, number INTEGER NOT NULL)')

      postgres.exec('INSERT INTO numbers (number) SELECT * from generate_series(11, 20) AS number')
      sleep 1

      postgres.exec('DELETE FROM numbers')

      partitions = kafka_take_messages('numbers', 20, collect_partitions: true)

      partitions.each do |partition, messages|
        # each key should have two messages, an insert followed by a delete
        messages.group_by(&:key).each do |key, messages_for_key|
          expect(messages_for_key.size).to eq(2)

          insert, delete = messages_for_key
          expect(insert.value).to_not be_nil
          expect(delete.value).to be_nil
        end
      end
    end

    example 'inserts and updates for a given key go to the same partition' do
      kazoo.create_topic('things', partitions: 2, replication_factor: 1)

      postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')

      result = postgres.exec('INSERT INTO things (thing) SELECT * from generate_series(11, 20) AS thing RETURNING id')
      ids = result.map {|row| row.fetch('id') }
      sleep 1

      # make sure there's no ordering dependency
      ids.shuffle!

      ids.each do |id|
        postgres.exec_params('UPDATE things SET thing = thing + 1 WHERE id = $1', [id])
      end

      partitions = kafka_take_messages('things', 20, collect_partitions: true)

      partitions.each do |partition, messages|
        # each key should have two messages, the second with an incremented value
        messages.group_by(&:key).each do |key, messages_for_key|
          expect(messages_for_key.size).to eq(2)

          insert, update = messages_for_key
          insert_value = fetch_int(decode_value(insert.value), 'thing')
          update_value = fetch_int(decode_value(update.value), 'thing')
          expect(update_value).to eq(insert_value + 1)
        end
      end
    end
  end

  describe 'unkeyed table' do
    example 'messages are partitioned evenly' do
      kazoo.create_topic('events', partitions: 2, replication_factor: 1)

      postgres.exec('CREATE TABLE events (event TEXT)')

      postgres.exec("INSERT INTO events (event) SELECT 'event ' || num AS event FROM generate_series(1, 100) AS num")
      sleep 1

      partitions = kafka_take_messages('events', 100, collect_partitions: true)
      expect(partitions.size).to eq(2)
      messages_0, messages_1 = partitions.values

      expect(messages_0.size).to be_within(20).of(messages_1.size)
    end
  end
end
