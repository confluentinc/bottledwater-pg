require 'spec_helper'

shared_examples 'smoke test' do |format, postgres_version|
  before(:context) do
    require 'test_cluster'
    TEST_CLUSTER.bottledwater_format = format
    TEST_CLUSTER.postgres_version = postgres_version
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

describe 'smoke test (JSON, Postgres 9.4)', functional: true, format: :json, postgres: '9.4' do
  it_should_behave_like 'smoke test', :json, '9.4'
end

describe 'smoke test (Avro, Postgres 9.4)', functional: true, format: :avro, postgres: '9.4' do
  it_should_behave_like 'smoke test', :avro, '9.4'
end

describe 'smoke test (JSON, Postgres 9.5)', functional: true, format: :json, postgres: '9.5' do
  it_should_behave_like 'smoke test', :json, '9.5'
end

describe 'smoke test (Avro, Postgres 9.5)', functional: true, format: :avro, postgres: '9.5' do
  it_should_behave_like 'smoke test', :avro, '9.5'
end
