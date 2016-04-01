require 'spec_helper'
require 'test_cluster'

describe 'outages', functional: true do
  after(:example) do
    TEST_CLUSTER.stop(dump_logs: false)
  end

  let(:postgres) { TEST_CLUSTER.postgres }

  example 'BW should not crash if Kafka is down' do
    pending 'make publish errors non-fatal'

    TEST_CLUSTER.start(without: [:kafka])

    postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
    postgres.exec('INSERT INTO things (thing) VALUES (42)')
    sleep 5

    expect(TEST_CLUSTER.bottledwater_running?).to be_truthy
  end

  example 'BW in Avro mode should not crash if the schema registry is down' do
    TEST_CLUSTER.bottledwater_format = :avro
    TEST_CLUSTER.start(without: [:'schema-registry'])

    postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
    postgres.exec('INSERT INTO things (thing) VALUES (42)')
    sleep 5

    expect(TEST_CLUSTER.bottledwater_running?).to be_truthy
  end
end
