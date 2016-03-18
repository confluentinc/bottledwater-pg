require 'spec_helper'
require 'format_contexts'
require File.join(File.dirname(__FILE__), 'type_specs')


# Arbitrary datetime to test with: first commit to Bottled Water (per Git)
# (plus invented fractional seconds to test roundtrip fidelity)
TEST_DATETIME = Time.new(2014, 12, 27, 17, 40, 15.123456, '+01:00')
# freeze "today" to avoid intermittent second-boundary failures
TODAY =         Time.new(2014, 12, 27, 0,  0,  0,         '+01:00')


module DateMatchers
  extend RSpec::Matchers::DSL

  matcher :equal_up_to_usec do |expected|
    match do |actual|
      actual.to_i == expected.to_i && actual.usec == expected.usec
    end

    diffable
  end

  matcher :equal_time_up_to_usec do |expected|
    # interpret `time` as if it was TODAY at the same wall clock time
    def same_time_today(time)
      TODAY + time.hour * 60 * 60 + time.min * 60 + time.sec + time.usec * 1e-6
    end

    match do |actual|
      today_expected = same_time_today(expected)
      today_actual = same_time_today(actual)

      expect(today_actual).to equal_up_to_usec(today_expected)
    end

    diffable
  end
end


shared_examples 'database schema support' do |format|
  # We only stop the cluster after all examples in the context have run, so
  # state in Postgres, Kafka and Bottled Water can leak between examples.  We
  # therefore need to make sure examples look at different tables, so they
  # don't affect each other.

  before(:context) do
    require 'test_cluster'
    TEST_CLUSTER.bottledwater_format = format
    TEST_CLUSTER.start
  end

  after(:context) do
    TEST_CLUSTER.stop
  end

  let(:postgres) { TEST_CLUSTER.postgres }

  def retrieve_roundtrip_message(type, value_str, as_key: false, length: nil)
    table_name = "test_#{as_key ? 'key' : 'value'}_#{type.gsub(/\W/, '_')}"

    lengthspec = "(#{length})" unless length.nil?
    keyspec = 'PRIMARY KEY' if as_key
    postgres.exec(%{CREATE TABLE "#{table_name}" (value #{type}#{lengthspec} NOT NULL #{keyspec})})
    postgres.exec_params(%{INSERT INTO "#{table_name}" (value) VALUES ($1)}, [value_str])
    sleep 1 # for topic to be created

    kafka_take_messages(table_name, 1).first
  end

  shared_examples 'roundtrip type' do |type, value, length: nil|
    example 'retrieve same value from Kafka as was stored in Postgres' do
      message = retrieve_roundtrip_message(type, value, length: length)

      row = decode_value(message.value)
      roundtrip_value = fetch_any(row, 'value')
      expect(roundtrip_value).to eq(value)
    end

    example 'retrieve same value from Kafka message key as was stored in Postgres' do
      message = retrieve_roundtrip_message(type, value, as_key: true, length: length)

      key = decode_key(message.key)
      roundtrip_value = fetch_any(key, 'value')
      expect(roundtrip_value).to eq(value)
    end
  end

  def time_from_time_since_start_of_day(day, hour: 0, min: 0, sec: 0, usec: 0)
    day + hour * 60 * 60 + min * 60 + sec + usec * 1e-6
  end

  def parse_bottledwater_date_format(date_hash, assume_offset: nil)
    # e.g. {"year"=>2016, "month"=>3, "day"=>15, "hour"=>21, "minute"=>59, "second"=>30, "micro"=>0, "zoneOffset"=>0}
    year = date_hash['year']
    month = date_hash['month']
    day = date_hash['day']

    hour = date_hash['hour']
    min = date_hash['minute']
    seconds = date_hash['second']
    micro = date_hash['micro']
    sec = (seconds || 0) + ((micro || 0) * 1e-6) unless seconds.nil? && micro.nil?

    offset = date_hash.fetch('zoneOffset', assume_offset)

    if !year.nil?
      if hour.nil? # year but no hour => date
        Date.new(year, month, day)
      else # year and hour => datetime
        Time.new(year, month, day, hour, min, sec, offset)
      end
    elsif ![hour, min, seconds, micro].compact.empty? # no year, at least one time component => time
      time_from_time_since_start_of_day(TODAY,
        hour: hour || 0,
        min: min || 0,
        sec: seconds || 0,
        usec: micro || 0)
    else # no year and no time
      raise ArgumentError, "invalid datetime hash: #{date_hash.inspect}"
    end
  end

  shared_examples 'date/time type' do |type, value = TEST_DATETIME|
    include DateMatchers

    example 'value comes back in a parseable form with µsec fidelity' do
      formatted = format(value)
      message = retrieve_roundtrip_message(type, formatted)

      row = decode_value(message.value)
      roundtrip_value = fetch_any(row, 'value')

      interpret_and_compare(roundtrip_value, value)
    end

    example 'value comes back in message key in a parseable form with µsec fidelity' do
      formatted = format(value)
      message = retrieve_roundtrip_message(type, formatted, as_key: true)

      key = decode_key(message.key)
      roundtrip_key = fetch_any(key, 'value')

      interpret_and_compare(roundtrip_key, value)
    end
  end

  shared_examples 'timestamp' do |type, value = TEST_DATETIME|
    include_examples 'date/time type', type, value

    def format(timestamp)
      timestamp.strftime('%FT%T.%N%:z')
    end

    def interpret_and_compare(received_hash, expected)
      parsed = parse_bottledwater_date_format(received_hash, assume_offset: assumed_offset)
      expect(parsed).to equal_up_to_usec(expected)
    end
  end
  shared_examples 'timestamp without time zone' do |value = TEST_DATETIME|
    let(:assumed_offset) { value.utc_offset }
    include_examples 'timestamp', 'timestamp without time zone', value
  end
  shared_examples 'timestamp with time zone' do |value = TEST_DATETIME|
    let(:assumed_offset) { nil }
    include_examples 'timestamp', 'timestamp with time zone', value
  end

  shared_examples 'date' do |value = TEST_DATETIME.to_date|
    include_examples 'date/time type', 'date', value

    def format(date)
      date.to_s
    end

    def interpret_and_compare(received_hash, expected)
      parsed = parse_bottledwater_date_format(received_hash)
      expect(parsed).to eq(expected)
    end
  end

  shared_examples 'time without time zone' do |value = TEST_DATETIME|
    include_examples 'date/time type', 'time without time zone', value

    def format(time)
      time.strftime('%T.%N%:z')
    end

    def interpret_and_compare(received_usec_since_start_of_day, expected)
      # interpret the received time in TODAY's timezone
      today_received = TODAY + received_usec_since_start_of_day * 1e-6

      expect(today_received).to equal_time_up_to_usec(expected)
    end
  end

  shared_examples 'time with time zone' do |value = TEST_DATETIME|
    include_examples 'date/time type', 'time with time zone', value

    def format(time)
      time.strftime('%T.%N%:z')
    end

    def interpret_and_compare(received_hash, expected)
      parsed = parse_bottledwater_date_format(received_hash)
      expect(parsed).to equal_time_up_to_usec(expected)
    end
  end

  shared_examples 'bit-string type' do |type, value = '00101010', length: nil|
    include_examples 'roundtrip type', type, value, length: length
  end

  shared_examples 'numeric type' do |type, value|
    include_examples 'roundtrip type', type, value
  end

  shared_examples 'string type' do |type, value, length: nil|
    include_examples 'roundtrip type', type, value, length: length
  end


  include_examples 'type specs'


  describe 'table with no columns' do
    after(:example) do
      # this is known to crash Postgres
      unless TEST_CLUSTER.healthy?
        TEST_CLUSTER.restart
      end
    end

    example 'sends empty messages' do
      xbug 'seems to crash Postgres!'

      table_name = 'zero_columns'

      postgres.exec(%{CREATE TABLE "#{table_name}" ()})
      postgres.exec(%{INSERT INTO "#{table_name}" DEFAULT VALUES})
      sleep 1 # for topic to be created

      message = kafka_take_messages(table_name, 1).first

      expect(message.value).to_not be_nil
      row = decode_value(message.value)
      expect(row).to be_empty
    end
  end

end


describe 'database schema support (JSON)', functional: true, format: :json do
  include_examples 'database schema support', :json
end

describe 'database schema support (Avro)', functional: true, format: :avro do
  include_examples 'database schema support', :avro
end
