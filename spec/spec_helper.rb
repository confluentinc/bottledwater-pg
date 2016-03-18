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

RSpec.configure do |config|
  config.include StringLogger
  config.include KnownBugs
  config.include KafkaHelpers

  config.around(:example) do |example|
    example.run
    if example.exception && @logger
      puts "Dumping logs due to failed example:"
      puts "-----------------------------------"
      puts @stringio.string
    end
  end
end
