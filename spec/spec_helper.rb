require 'logger'
require 'stringio'

module StringLogger
  def logger
    @stringio ||= StringIO.new
    @logger ||= Logger.new(@stringio)
  end

  def dump_logs!
    puts @stringio.string
  end
end

RSpec.configure do |config|
  config.include StringLogger

  config.around(:example) do |example|
    example.run
    if example.exception
      puts "Dumping logs due to failed example:"
      puts "-----------------------------------"
      dump_logs!
    end
  end
end
