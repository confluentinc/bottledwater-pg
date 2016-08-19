require 'spec_helper'
require 'test_cluster'

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

    example 'schemas other than "public" are included in the topic name' do
      postgres.exec('CREATE SCHEMA myapp')
      postgres.exec('CREATE TABLE myapp.things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
      postgres.exec('INSERT INTO myapp.things (thing) VALUES (42)')
      sleep 1

      expect(kazoo.topics).to have_key('myapp.things')
    end

    example 'table names with non-alphanumeric characters create a topic based on the sanitised name' do
      silly_name = 'flobble-biscuits?whatisthetime/hahahaha'

      postgres.exec %(CREATE TABLE "#{silly_name}" (thing SERIAL NOT NULL PRIMARY KEY))
      postgres.exec %(INSERT INTO "#{silly_name}" DEFAULT VALUES)
      sleep 1

      expect(kazoo.topics).to include(match(/flobble.*biscuits.*whatisthetime.*hahahaha/))
    end

    example 'table names with non-ASCII characters create a topic based on the sanitised name' do
      unicode_name = 'crêpes'

      postgres.exec %(CREATE TABLE "#{unicode_name}" (thing SERIAL NOT NULL PRIMARY KEY))
      postgres.exec %(INSERT INTO "#{unicode_name}" DEFAULT VALUES)

      sleep 1

      expect(kazoo.topics).to include('cr_c3__aa_pes')
    end

    example 'supports table names up to Postgres max identifier length' do
      long_name = 'z' * postgres_max_identifier_length

      postgres.exec %(CREATE TABLE "#{long_name}" (thing SERIAL NOT NULL PRIMARY KEY))
      postgres.exec %(INSERT INTO "#{long_name}" DEFAULT VALUES)
      sleep 1

      expect(kazoo.topics).to include(long_name)
    end
  end

  describe 'with topic autocreate enabled and --topic-prefix=bottledwater' do
    before(:context) do
      TEST_CLUSTER.bottledwater_topic_prefix = 'bottledwater'
      TEST_CLUSTER.start
    end

    after(:context) do
      TEST_CLUSTER.stop
    end

    after(:example) { kazoo.reset_metadata }

    example 'topic name has the prefix prepended' do
      postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
      postgres.exec('INSERT INTO things (thing) VALUES (42)')
      sleep 1

      expect(kazoo.topics).to have_key('bottledwater.things')
    end

    example 'schemas other than "public" are included in the topic name, after the prefix' do
      postgres.exec('CREATE SCHEMA myapp')
      postgres.exec('CREATE TABLE myapp.things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
      postgres.exec('INSERT INTO myapp.things (thing) VALUES (42)')
      sleep 1

      expect(kazoo.topics).to have_key('bottledwater.myapp.things')
    end
  end

  describe "with topic autocreate disabled and --on-error=exit" do
    before(:context) do
      TEST_CLUSTER.kafka_auto_create_topics_enable = false
      TEST_CLUSTER.bottledwater_on_error = :exit

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

    example 'inserting rows in a new table crashes Bottled Water' do
      expect(TEST_CLUSTER.bottledwater_running?).to be_truthy

      postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
      postgres.exec('INSERT INTO things (thing) VALUES (42)')
      sleep 5

      expect(TEST_CLUSTER.bottledwater_running?).to be_falsy
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

    example 'adding a primary key to a table then inserting crashes Bottled Water' do
      # We test this separately from the previous ALTER TABLE ... ADD COLUMN
      # because when adding a new primary key, Postgres first creates a
      # temporary table, which Bottled Water picks up as a new table to stream.

      expect(TEST_CLUSTER.bottledwater_running?).to be_truthy

      kazoo.create_topic('products', partitions: 1, replication_factor: 1)
      sleep 1

      postgres.exec('CREATE TABLE products (sku INTEGER NOT NULL)')
      postgres.exec('INSERT INTO products (sku) VALUES (31)')
      sleep 1
      postgres.exec('ALTER TABLE products ADD COLUMN id SERIAL PRIMARY KEY')
      postgres.exec('INSERT INTO products (sku) VALUES (42)')

      sleep 5

      expect(TEST_CLUSTER.bottledwater_running?).to be_falsy
    end
  end

  describe "with topic autocreate disabled and --on-error=log" do
    before(:context) do
      TEST_CLUSTER.kafka_auto_create_topics_enable = false
      TEST_CLUSTER.bottledwater_on_error = :log

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

    example 'inserting rows in a new table does not crash Bottled Water' do
      expect(TEST_CLUSTER.bottledwater_running?).to be_truthy

      postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
      postgres.exec('INSERT INTO things (thing) VALUES (42)')
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
