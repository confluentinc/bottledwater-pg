require 'spec_helper'

describe 'topics', functional: true do
  let(:postgres) { TEST_CLUSTER.postgres }
  let(:kazoo) { TEST_CLUSTER.kazoo }

  after(:example) do
    # since we're testing crashes...
    unless TEST_CLUSTER.healthy?
      TEST_CLUSTER.restart(dump_logs: false)
    end
  end


  describe 'with topic autocreate enabled' do
    before(:context) do
      require 'test_cluster'
      TEST_CLUSTER.start
    end

    after(:context) do
      TEST_CLUSTER.stop
    end

    after(:example) { kazoo.reset_metadata }

    example 'creating a table does not create a topic (until some rows are inserted)' do
      postgres.exec('CREATE TABLE items (id SERIAL PRIMARY KEY, item TEXT)')
      sleep 1

      expect(kazoo.topics).not_to have_key('items')
    end

    example 'inserting rows in a new table creates a Kafka topic named after the table' do
      postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
      postgres.exec('INSERT INTO things (thing) VALUES (42)')
      sleep 1

      expect(kazoo.topics).to have_key('things')
    end

    example 'creating a table with a silly name creates a topic based on the sanitised name' do
      known_bug 'crashes Postgres', 'https://github.com/confluentinc/bottledwater-pg/issues/64'

      silly_name = 'flobble-biscuits?whatisthetime/hahahaha'

      postgres.exec %(CREATE TABLE "#{silly_name}" (thing SERIAL NOT NULL PRIMARY KEY))
      postgres.exec %(INSERT INTO "#{silly_name}" DEFAULT VALUES)
      sleep 1

      expect(kazoo.topics).to include(match(/flobble/))
    end
  end

  describe 'with topic autocreate disabled' do
    before(:context) do
      require 'test_cluster'
      TEST_CLUSTER.kafka_auto_create_topics_enable = false
      TEST_CLUSTER.start
    end

    after(:context) do
      TEST_CLUSTER.stop
    end

    example 'inserting rows in a new table does not crash Bottled Water' do
      pending 'make publish errors non-fatal'

      expect(TEST_CLUSTER.bottledwater_running?).to be_truthy

      postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
      postgres.exec('INSERT INTO things (thing) VALUES (42)')
      sleep 5

      expect(TEST_CLUSTER.bottledwater_running?).to be_truthy
    end

    example 'inserting rows in a new table after creating the topic does not crash Bottled Water' do
      kazoo.create_topic('items', partitions: 1, replication_factor: 1)
      sleep 1

      postgres.exec('CREATE TABLE items (id SERIAL PRIMARY KEY, item INTEGER NOT NULL)')
      postgres.exec('INSERT INTO items (item) VALUES (42)')
      sleep 5

      expect(TEST_CLUSTER.bottledwater_running?).to be_truthy
    end

    example 'renaming a table, creating the new topic, then inserting does not crash Bottled Water' do
      expect(TEST_CLUSTER.bottledwater_running?).to be_truthy

      kazoo.create_topic('users', partitions: 1, replication_factor: 1)
      kazoo.create_topic('members', partitions: 1, replication_factor: 1)
      sleep 1

      postgres.exec('CREATE TABLE users (id SERIAL PRIMARY KEY, age INTEGER NOT NULL)')
      postgres.exec('INSERT INTO users (age) VALUES (31)')
      sleep 1
      postgres.exec('ALTER TABLE users RENAME TO members')
      postgres.exec('INSERT INTO members (age) VALUES (42)')

      sleep 5

      expect(TEST_CLUSTER.bottledwater_running?).to be_truthy
    end

    example 'altering table schema then inserting does not crash Bottled Water' do
      expect(TEST_CLUSTER.bottledwater_running?).to be_truthy

      kazoo.create_topic('customers', partitions: 1, replication_factor: 1)
      sleep 1

      postgres.exec('CREATE TABLE customers (age INTEGER NOT NULL)')
      postgres.exec('INSERT INTO customers (age) VALUES (31)')
      sleep 1
      postgres.exec('ALTER TABLE customers ADD COLUMN name TEXT NULL')
      postgres.exec("INSERT INTO customers (age, name) VALUES (42, 'Ron Swanson')")

      sleep 5

      expect(TEST_CLUSTER.bottledwater_running?).to be_truthy
    end

    example 'adding a primary key to a table then inserting does not crash Bottled Water' do
      # We test this separately from the previous ALTER TABLE ... ADD COLUMN
      # because when adding a new primary key, Postgres first creates a
      # temporary table, which Bottled Water picks up as a new table to stream.
      pending 'make publish errors non-fatal'

      expect(TEST_CLUSTER.bottledwater_running?).to be_truthy

      kazoo.create_topic('products', partitions: 1, replication_factor: 1)
      sleep 1

      postgres.exec('CREATE TABLE products (sku INTEGER NOT NULL)')
      postgres.exec('INSERT INTO products (sku) VALUES (31)')
      sleep 1
      postgres.exec('ALTER TABLE products ADD COLUMN id SERIAL PRIMARY KEY')
      postgres.exec('INSERT INTO products (sku) VALUES (42)')

      sleep 5

      expect(TEST_CLUSTER.bottledwater_running?).to be_truthy
    end
  end
end
