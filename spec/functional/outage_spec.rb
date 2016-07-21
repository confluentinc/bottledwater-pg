require 'spec_helper'
require 'test_cluster'

describe 'outages', functional: true do
  after(:example) do
    TEST_CLUSTER.stop(dump_logs: false)
  end

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
  end
end
