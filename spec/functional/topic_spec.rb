require 'spec_helper'

describe 'topics', functional: true do
  let(:postgres) { TEST_CLUSTER.postgres }
  let(:kazoo) { TEST_CLUSTER.kazoo }

  after(:example) { kazoo.reset_metadata }

  describe 'with topic autocreate enabled' do
    before(:context) do
      require 'test_cluster'
      TEST_CLUSTER.start
    end

    after(:context) do
      TEST_CLUSTER.stop
    end

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
  end
end
