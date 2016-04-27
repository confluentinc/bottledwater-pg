require 'spec_helper'

shared_examples 'smoke test' do |format|
  before(:context) do
    require 'test_cluster'
    TEST_CLUSTER.bottledwater_format = format
    TEST_CLUSTER.start
  end

  after(:context) do
    TEST_CLUSTER.stop
  end

  let(:postgres) { TEST_CLUSTER.postgres }

  example 'inserting rows in Postgres should publish messages to Kafka' do
    postgres.exec('CREATE TABLE things (id SERIAL PRIMARY KEY, thing INTEGER NOT NULL)')
    postgres.exec('INSERT INTO things (thing) SELECT * FROM generate_series(1, 10) AS thing')
    sleep 1

    messages = kafka_take_messages('things', 10)

    expect(messages.size).to eq 10
  end
end

describe 'smoke test (JSON)', functional: true, format: :json do
  it_should_behave_like 'smoke test', :json
end

describe 'smoke test (Avro)', functional: true, format: :avro do
  it_should_behave_like 'smoke test', :avro
end
