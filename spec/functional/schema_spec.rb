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

  def retrieve_roundtrip_message(type, value_str, length = nil)
    table_name = "test_#{type.gsub(/\W/, '_')}"

    lengthspec = "(#{length})" unless length.nil?
    postgres.exec(%{CREATE TABLE "#{table_name}" (value #{type}#{lengthspec} NOT NULL)})
    postgres.exec_params(%{INSERT INTO "#{table_name}" (value) VALUES ($1)}, [value_str])
    sleep 1 # for topic to be created

    kafka_take_messages(table_name, 1).first
  end

  shared_examples 'roundtrip type' do |type, value, length = nil|
    example 'retrieve same value from Kafka as was stored in Postgres' do
      message = retrieve_roundtrip_message(type, value, length)

      row = decode_value(message.value)
      roundtrip_value = fetch_any(row, 'value')
      expect(roundtrip_value).to eq(value)
    end
  end

  def time_from_time_since_start_of_day(day, hour: 0, min: 0, sec: 0, usec: 0)
    day + hour * 60 * 60 + min * 60 + sec + usec * 1e-6
  end

  def parse_bottledwater_date_format(date_hash, assume_offset = nil)
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

  shared_examples 'timestamp' do |type, value = TEST_DATETIME|
    include DateMatchers

    def format_timestamp(timestamp)
      timestamp.strftime('%FT%T.%N%:z')
    end

    example 'value comes back in a parseable form with µsec fidelity' do
      formatted = format_timestamp(value)
      message = retrieve_roundtrip_message(type, formatted)

      row = decode_value(message.value)
      roundtrip_value = fetch_any(row, 'value')
      parsed_value = parse_bottledwater_date_format(roundtrip_value, assumed_offset)
      expect(parsed_value).to equal_up_to_usec(value)
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
    include DateMatchers

    def format_date(date)
      date.to_s
    end

    example 'value comes back in a parseable form with µsec fidelity' do
      formatted = format_date(value)
      message = retrieve_roundtrip_message('date', formatted)

      row = decode_value(message.value)
      roundtrip_value = fetch_any(row, 'value')
      parsed_value = parse_bottledwater_date_format(roundtrip_value)
      expect(parsed_value).to eq(value)
    end
  end

  shared_examples 'time without time zone' do |value = TEST_DATETIME|
    include DateMatchers

    def format_time(time)
      time.strftime('%T.%N%:z')
    end

    before(:example) { postgres.exec("SET timezone = 'UTC'") }
    after(:example) { postgres.exec('SET timezone = DEFAULT') }

    example 'value comes back in a parseable form with µsec fidelity' do
      formatted = format_time(value)
      message = retrieve_roundtrip_message('time without time zone', formatted)

      row = decode_value(message.value)
      usec_since_start_of_day = fetch_any(row, 'value')

      # interpret the received time in `value`'s timezone
      today_received = TODAY + usec_since_start_of_day * 1e-6

      expect(today_received).to equal_time_up_to_usec(value)
    end
  end

  shared_examples 'time with time zone' do |value = TEST_DATETIME|
    include DateMatchers

    def format_time(time)
      time.strftime('%T.%N%:z')
    end

    example 'value comes back in a parseable form with µsec fidelity' do
      formatted = format_time(value)
      message = retrieve_roundtrip_message('time with time zone', formatted)

      row = decode_value(message.value)
      roundtrip_value = fetch_any(row, 'value')
      parsed_value = parse_bottledwater_date_format(roundtrip_value)
      expect(parsed_value).to equal_up_to_usec(value)
    end
  end

  shared_examples 'bit-string type' do |type, value = '00101010', length = nil|
    include_examples 'roundtrip type', type, value, length
  end

  shared_examples 'numeric type' do |type, value = 42|
    include_examples 'roundtrip type', type, value
  end

  shared_examples 'string type' do |type, value = 'Hello, world!', length = nil|
    include_examples 'roundtrip type', type, value, length
  end


  include_examples 'type specs'

end


describe 'database schema support (JSON)', functional: true do
  include_context 'JSON format'

  include_examples 'database schema support', :json
end

describe 'database schema support (Avro)', functional: true do
  include_context 'Avro format'

  include_examples 'database schema support', :avro
end
