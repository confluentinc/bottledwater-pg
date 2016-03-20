require 'optparse'
require 'pg'
require 'set'

require File.expand_path(File.join(File.dirname(__FILE__), '..', 'test_cluster'))

indent_level = 0

OptionParser.new do |opts|
  opts.banner = "Usage: #$PROGRAM_NAME [options]"

  opts.on('-iLEVEL', '--indent=LEVEL', 'Indent level') do |indent|
    indent_level = Integer(indent)
  end
end.parse!

INDENT = (' ' * indent_level).freeze

def iputs(level, *args)
  print(INDENT)
  print(' ' * level) unless level == 0
  puts(*args)
end

TEST_CLUSTER.bottledwater_format = :json # skip schema registry
TEST_CLUSTER.start
at_exit { TEST_CLUSTER.stop }
pg = TEST_CLUSTER.postgres

types = pg.exec(<<-SQL)
  SELECT
    -- get e.g. 'timestamp with time zone' instead of 'timestamptz'
    format_type(t.oid, NULL) AS name,
    t.typname AS typname,
    t.typcategory AS typcategory
  FROM pg_type t
  WHERE
    -- exclude composite and pseudotypes
    t.typtype NOT IN ('c', 'p')
    -- exclude invisible types
    AND pg_type_is_visible(t.oid)
    -- exclude 'element' types
    AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
  ORDER BY name
SQL

# Add types to this list if they require a more specific way to create a
# test value than can be inferred from just their 'typcategory'
#
# If the value is a string, it will be escaped (via String#inspect) and
# included in the generated code as a string literal.  If the value is wrapped
# in a single-element Array, it will be included unescaped in the generated
# code.  See function #genvalue below for reference.
CUSTOM_VALUE_TYPES = {
  'date'    => ['TEST_DATETIME.to_date'],

  # inet types
  'cidr'    => '192.168.1.0/24',
  'inet'    => '192.168.1.1/24',
  'macaddr' => '08:00:2b:01:02:03',

  # range types
  'int4range' => '[1,5)',
  'int8range' => '[1,5)',
  'numrange' => '[1,5)',
  'daterange' => '[1837-06-20,1901-01-22)', # the reign of Queen Victoria
  'tsrange' =>   '["1837-06-20 00:00:00","1901-01-22 00:00:00")',
  'tstzrange' => '["1837-06-20 00:00:00+00","1901-01-22 00:00:00+00")',
}
def genvalue(value)
  case value
  when Array
    raise "Bad value expression: #{value.inspect}" if value.size != 1
    value.first
  else
    value.inspect
  end
end

# add types to this list if the test table needs to explicitly specify the
# length of the type
BOUNDED_LENGTH_TYPES = Set[*%w(
  bit
  character
)]

# add types to this list if they're too obscure to bother filing an issue for
# not supporting them (e.g. Postgres internals)
INTERNAL_TYPES = Set[*%w(
  abstime
  "char"
  name
  pg_node_tree
  regclass
  regconfig
  regdictionary
  regoper
  regoperator
  regproc
  regprocedure
  regtype
  reltime
  tinterval
  txid_snapshot
  unknown
  xid
)]

# add types to this list when their lack of support is documented, preferably
# in the form of a Github issue
KNOWN_BUGS = {
  'numeric' => ['replaced by zero', 'https://github.com/confluentinc/bottledwater-pg/issues/4'],
}

# only use this list during development, otherwise file an issue!
UNKNOWN_BUGS = {
  'money' => 'mysteriously gets multiplied by 100',
}

def print_constants(level)
  iputs level, %(# Arbitrary datetime to test with: first commit to Bottled Water (per Git))
  iputs level, %(# (plus invented fractional seconds to test roundtrip fidelity))
  iputs level, %(TEST_DATETIME = Time.new(2014, 12, 27, 17, 40, 15.123456, '+01:00'))
end

def print_examples(level, type)
  name = type.fetch('name')

  if INTERNAL_TYPES.include?(name)
    iputs level, %(example('internal type not supported') {})
    return
  end

  if info = KNOWN_BUGS[name]
    problem, url = info
    iputs level, %(before :example do)
    iputs level, %(  known_bug #{problem.inspect}, #{url.inspect})
    iputs level, %(end)
    puts
    # fall through
  elsif problem = UNKNOWN_BUGS[name]
    iputs level, %(before :example do)
    iputs level, %(  xbug #{problem.inspect})
    iputs level, %(end)
    puts
    # fall through
  end

  value = CUSTOM_VALUE_TYPES[name]

  # see http://www.postgresql.org/docs/9.5/static/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE
  case type.fetch('typcategory')
  when 'B' # boolean
    iputs level,   %(include_examples 'roundtrip type', #{name.inspect}, true)
  when 'V' # bit-string
    if BOUNDED_LENGTH_TYPES.include?(name)
      value = '1110' if value.nil?
      length = value.size
      iputs level, %(include_examples 'bit-string type', #{name.inspect}, #{genvalue(value)}, length: #{length})
    else
      iputs level, %(include_examples 'bit-string type', #{name.inspect})
    end
  when 'N' # numeric
    value = 42 if value.nil?
    iputs level,   %(include_examples 'numeric type', #{name.inspect}, #{genvalue(value)})
  when 'S' # string
    if BOUNDED_LENGTH_TYPES.include?(name)
      value = 'Hello' if value.nil?
      length = value.size
      iputs level, %(include_examples 'string type', #{name.inspect}, #{genvalue(value)}, length: #{length})
    else
      value = 'Hello, world!' if value.nil?
      iputs level, %(include_examples 'string type', #{name.inspect}, #{genvalue(value)})
    end
  when 'D' # date/time
    value = ['TEST_DATETIME'] if value.nil?
    iputs level,   %(include_examples #{name.inspect}, #{genvalue(value)})
  when 'I' # inet
    raise "Please specify custom literal for inet type #{name}" if value.nil?
    iputs level,   %(include_examples 'roundtrip type', #{name.inspect}, #{genvalue(value)})
  when 'R' # range
    raise "Please specify custom literal for range type #{name}" if value.nil?
    iputs level,   %(include_examples 'roundtrip type', #{name.inspect}, #{genvalue(value)})
  when 'T' # timespan
    value = '01:23:45.123456' if value.nil?
    iputs level,   %(include_examples 'interval type', #{name.inspect}, #{genvalue(value)})
  else
    iputs level,   %(pending('should have specs') { fail 'spec not yet implemented for typcategory #{type['typcategory']}' })
  end
end

iputs 0,     ('#' * 80)
iputs 0,     %(### This file is automatically generated by #$PROGRAM_NAME)
iputs 0,     %(### It is intended to be human readable (hence being checked into Git), but)
iputs 0,     %(### not manually edited.)
iputs 0,     %(### This is to make it easier to maintain tests for all supported Postgres)
iputs 0,     %(### types, even as extensions or new Postgres versions add new types.)
iputs 0,     ('#' * 80)
puts
print_constants(0)
puts
iputs 0,     %(shared_examples 'type specs' do)
puts
types.each do |type|
  name = type.fetch('name')

  iputs 0,   %(  describe '#{name}' do)
  print_examples(4, type)
  iputs   0, %(  end)
  puts
end
iputs 0,     %(end)
