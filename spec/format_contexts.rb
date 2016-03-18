require 'avro/registered_schema_decoder'


shared_context 'JSON format' do
  def parse(*args); JSON.parse(*args); end
  alias decode_key parse
  alias decode_value parse

  def fetch_int(object, name)
    object.fetch(name).fetch('int')
  end

  def fetch_string(object, name)
    object.fetch(name).fetch('string')
  end

  def fetch_any(object, name)
    object.fetch(name).values.first
  end
end

shared_context 'Avro format' do
  let(:decoder) do
    Avro::RegisteredSchemaDecoder.new(
      TEST_CLUSTER.schema_registry_url,
      logger: logger)
  end

  def decode_key(*args)
    decoder.decode_key(*args)
  end

  def decode_value(*args)
    decoder.decode_value(*args)
  end

  def fetch_entry(object, name)
    object.fetch(name)
  end
  alias fetch_int fetch_entry
  alias fetch_string fetch_entry
  alias fetch_any fetch_entry
end
