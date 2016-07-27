require 'spec_helper'
require 'format_contexts'
require File.join(File.dirname(__FILE__), 'type_specs')


# freeze "today" to avoid intermittent second-boundary failures
TODAY = Time.new(2015, 12, 27, 0, 0, 0, '+02:00')


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
  let(:kazoo) { TEST_CLUSTER.kazoo }

  # We could just rely on topic autocreate, but then we have to wait an unknown
  # amount of time after inserting the row for Kafka to create the topic before
  # we can try consuming from it, which is slow and unreliable.  Instead we
  # explicitly create the topic beforehand.
  def create_topic(name, config: nil)
    kazoo.create_topic(name, partitions: 1, replication_factor: 1, config: config)

    # ... except that Kafka seems to take a while to notice the change in Zookeeper...
    sleep 0.1
  end

  def retrieve_roundtrip_message(
      type, value_str,
      as_key: false, length: nil,
      table_name: "test_#{as_key ? 'key' : 'value'}_#{type.gsub(/\W/, '_')}",
      column_name: :value)

    create_topic(table_name)

    lengthspec = "(#{length})" unless length.nil?
    if as_key
      keyspec = 'PRIMARY KEY'
    else
      # have to make sure the table has a primary key, so Kafka 0.9 won't
      # reject the message (unkeyed message to a compacted topic)
      keyspec = ', id SERIAL PRIMARY KEY NOT NULL'
    end
    postgres.exec(%{CREATE TABLE "#{table_name}" ("#{column_name}" #{type}#{lengthspec} NOT NULL #{keyspec})})
    postgres.exec_params(%{INSERT INTO "#{table_name}" ("#{column_name}") VALUES ($1)}, [value_str])

    kafka_take_messages(table_name, 1).first
  end

  shared_examples 'roundtrip type' do |type, value, length: nil, as_key: true|
    example 'retrieve same value from Kafka as was stored in Postgres' do
      message = retrieve_roundtrip_message(type, value, length: length)

      row = decode_value(message.value)
      roundtrip_value = fetch_any(row, 'value')
      expect(roundtrip_value).to eq(value)
    end

    if as_key
      example 'retrieve same value from Kafka message key as was stored in Postgres' do
        message = retrieve_roundtrip_message(type, value, as_key: true, length: length)

        key = decode_key(message.key)
        roundtrip_value = fetch_any(key, 'value')
        expect(roundtrip_value).to eq(value)
      end
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

  shared_examples 'date/time type' do |type, value|
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

  shared_examples 'timestamp' do |type, value|
    include_examples 'date/time type', type, value

    def format(timestamp)
      timestamp.strftime('%FT%T.%N%:z')
    end

    def interpret_and_compare(received_hash, expected)
      parsed = parse_bottledwater_date_format(received_hash, assume_offset: assumed_offset)
      expect(parsed).to equal_up_to_usec(expected)
    end
  end
  shared_examples 'timestamp without time zone' do |value|
    let(:assumed_offset) { value.utc_offset }
    include_examples 'timestamp', 'timestamp without time zone', value
  end
  shared_examples 'timestamp with time zone' do |value|
    let(:assumed_offset) { nil }
    include_examples 'timestamp', 'timestamp with time zone', value
  end

  shared_examples 'date' do |value|
    include_examples 'date/time type', 'date', value

    def format(date)
      date.to_s
    end

    def interpret_and_compare(received_hash, expected)
      parsed = parse_bottledwater_date_format(received_hash)
      expect(parsed).to eq(expected)
    end
  end

  shared_examples 'time without time zone' do |value|
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

  shared_examples 'time with time zone' do |value|
    include_examples 'date/time type', 'time with time zone', value

    def format(time)
      time.strftime('%T.%N%:z')
    end

    def interpret_and_compare(received_hash, expected)
      parsed = parse_bottledwater_date_format(received_hash)
      expect(parsed).to equal_time_up_to_usec(expected)
    end
  end

  shared_examples 'interval type' do |type, value|
    # returns a Postgres interval literal equivalent to the received hash
    def parse_bottledwater_interval_format(interval_hash)
      second = interval_hash.fetch('second', 0)
      micro = interval_hash.fetch('micro', 0)

      %w(
        year
        month
        day
        hour
        minute
      ).
      map do |name|
        part = interval_hash.fetch(name, 0)
        "#{part} #{name}s"
      end.
      join(' ') + " #{second + 1e-6 * micro} seconds"
    end

    # rather than bothering with formatting the interval, let's just check that
    # Postgres interprets the value we parsed the same as the original one we
    # sent it
    def postgres_canonicalize(type, formatted)
      postgres.exec_params("SELECT $1::#{type}", [formatted]).first.values.first
    end

    example 'value comes back in a parseable form with µsec fidelity' do
      message = retrieve_roundtrip_message(type, value)

      row = decode_value(message.value)
      received_interval = parse_bottledwater_interval_format(fetch_any(row, 'value'))
      expect(postgres_canonicalize(type, received_interval)).to eq(value)
    end

    example 'value comes back in message key in a parseable form with µsec fidelity' do
      message = retrieve_roundtrip_message(type, value, as_key: true)

      key = decode_key(message.key)
      received_interval = parse_bottledwater_interval_format(fetch_any(key, 'value'))
      expect(postgres_canonicalize(type, received_interval)).to eq(value)
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

  shared_examples 'geometric type' do |type, value|
    # geometric types can't be in a primary key because they don't support the
    # right index types
    include_examples 'roundtrip type', type, value, as_key: false
  end

  shared_examples 'JSON type' do |type, value|
    # JSON types can't be in a primary key because they don't support a default
    # operator class

    example 'retrieve same value from Kafka as was stored in Postgres' do
      message = retrieve_roundtrip_message(type, JSON.generate(value))

      row = decode_value(message.value)
      roundtrip_value = JSON.parse(fetch_string(row, 'value'))
      expect(roundtrip_value).to eq(value)
    end
  end

  shared_examples 'binary type' do |type|
    let(:bytes) { [0xbe, 0xef, 0x00, 0xca, 0xfe] }
    let(:bytestring) { bytes.pack('c*') }

    # e.g. [0xbe, 0xef] -> '\xbeef'
    def encode_postgres_literal(bytes)
      '\x' + bytes.map {|b| '%02x' % b }.join('')
    end

    example 'retrieve same bytes from Kafka as were stored in Postgres' do
      literal = encode_postgres_literal(bytes)
      message = retrieve_roundtrip_message(type, literal)

      row = decode_value(message.value)
      roundtrip_bytestring = fetch_bytes(row, 'value')

      if :json == format
        known_bug 'truncates at null byte', 'https://github.com/confluentinc/bottledwater-pg/issues/70'
      end

      expect(roundtrip_bytestring).to eq(bytestring)
    end

    example 'retrieve same bytes from Kafka message key as were stored in Postgres' do
      literal = encode_postgres_literal(bytes)
      message = retrieve_roundtrip_message(type, literal, as_key: true)

      key = decode_key(message.key)
      roundtrip_bytestring = fetch_bytes(key, 'value')

      if :json == format
        known_bug 'truncates at null byte', 'https://github.com/confluentinc/bottledwater-pg/issues/70'
      end

      expect(roundtrip_bytestring).to eq(bytestring)
    end
  end


  include_examples 'type specs'


  describe 'array types' do
    describe 'int[]' do
      include_examples 'roundtrip type', 'int[]', '{1,2,3,4}'
    end
    describe 'text[]' do
      include_examples 'roundtrip type', 'text[]', '{1,two,"three, four"}'
    end
  end


  describe 'column names' do
    example 'supports column names up to Postgres max identifier length in row' do
      long_name = 'z' * postgres_max_identifier_length

      message = retrieve_roundtrip_message(
          'int', 42,
          table_name: :test_long_name_value, column_name: long_name)

      value = decode_value(message.value)
      expect(value).to have_key(long_name)
    end

    example 'supports column names up to Postgres max identifier length in key' do
      long_name = 'z' * postgres_max_identifier_length

      message = retrieve_roundtrip_message(
          'int', 42,
          as_key: true,
          table_name: :test_long_name_key, column_name: long_name)

      key = decode_key(message.key)
      expect(key).to have_key(long_name)
    end

    example 'sanitises non-alphanumeric characters in row' do
      message = retrieve_roundtrip_message(
          'int', 42,
          table_name: :test_silly_name_value, column_name: 'person.name/surname')

      value = decode_value(message.value)
      expect(value.keys).to include(match(/person.*name.*surname/))
    end

    example 'sanitises non-alphanumeric characters in key' do
      message = retrieve_roundtrip_message(
          'int', 42,
          as_key: true,
          table_name: :test_silly_name_key, column_name: 'person.name/surname')

      key = decode_key(message.key)
      expect(key.keys).to include(match(/person.*name.*surname/))
    end
  end


  describe 'table with no columns' do
    after(:example) do
      # some of these are known to terminate the replication connection,
      # causing Bottled Water to exit
      unless TEST_CLUSTER.healthy?
        TEST_CLUSTER.restart(dump_logs: false)
      end
    end

    example 'publishes dummy messages' do
      table_name = 'zero_columns'

      create_topic(table_name, config: {'cleanup.policy' => 'delete'})

      postgres.exec(%{CREATE TABLE "#{table_name}" ()})
      postgres.exec(%{INSERT INTO "#{table_name}" DEFAULT VALUES})

      message = kafka_take_messages(table_name, 1).first

      expect(message.value).to_not be_nil
      row = decode_value(message.value)
      expect(row).to be_a(Hash)
    end

    example 'if columns are added later, drops the dummy data' do
      table_name = 'zero_columns_initially'

      create_topic(table_name, config: {'cleanup.policy' => 'delete'})

      postgres.exec(%{CREATE TABLE "#{table_name}" ()})
      postgres.exec(%{INSERT INTO "#{table_name}" DEFAULT VALUES})

      postgres.exec(%{ALTER TABLE "#{table_name}" ADD COLUMN stuff TEXT})
      postgres.exec(%{INSERT INTO "#{table_name}" (stuff) VALUES ('have some data')})

      message = kafka_take_messages(table_name, 2).last

      expect(message.value).to_not be_nil
      row = decode_value(message.value)
      expect(row).to have_key('stuff')
      expect(row.size).to be(1)
    end

    example 'if you remove all columns from a table, publishes dummy messages' do
      known_bug 'terminates replication stream', 'https://github.com/confluentinc/bottledwater-pg/issues/97'

      table_name = 'zero_columns_eventually'

      create_topic(table_name, config: {'cleanup.policy' => 'delete'})

      postgres.exec(%{CREATE TABLE "#{table_name}" (stuff TEXT NOT NULL)})
      postgres.exec(%{INSERT INTO "#{table_name}" (stuff) VALUES ('have some data')})

      postgres.exec(%{ALTER TABLE "#{table_name}" DROP COLUMN stuff})
      postgres.exec(%{INSERT INTO "#{table_name}" DEFAULT VALUES})

      message = kafka_take_messages(table_name, 2).last

      expect(message.value).to_not be_nil
      row = decode_value(message.value)
      expect(row).to_not have_key('stuff')
      expect(row).to be_a(Hash)
    end
  end

end


describe 'database schema support (JSON)', functional: true, format: :json do
  include_examples 'database schema support', :json
end

describe 'database schema support (Avro)', functional: true, format: :avro do
  include_examples 'database schema support', :avro
end
