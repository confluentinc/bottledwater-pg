require 'kafka_helpers'

require 'logger'
require 'stringio'

module StringLogger
  def logger
    @stringio ||= StringIO.new
    @logger ||= Logger.new(@stringio)
  end
end

module KnownBugs
  def known_bug(description, url)
    pending "#{description}: see #{url}"
  end

  def xbug(description)
    pending "#{description} (TODO file a bug!)"
  end
end

module Constants
  # Need to define methods rather than just regular constants, because
  # apparently RSpec's `config.include` doesn't include constants.

  # see https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
  def postgres_max_identifier_length; 63; end
end

RSpec.configure do |config|
  config.include StringLogger
  config.include KnownBugs
  config.include KafkaHelpers
  config.include Constants

  config.around(:example) do |example|
    example.run
    if example.exception && @logger
      puts "Dumping logs due to failed example:"
      puts "-----------------------------------"
      puts @stringio.string
    end
  end
end
