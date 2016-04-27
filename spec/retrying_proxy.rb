# Proxy that retries methods of the proxied object if they throw an exception.
#
# Optionally logs each retry, along with the exception that triggered it.
#
# Mostly intended for retrying operations that are known to intermittently fail,
# such as calls to the Docker API where we've observed sometimes receiving partial
# JSON responses, causing a parse failure.
#
# Retrying a side-effecting method will repeat the side-effects.  Caveat emptor!
class RetryingProxy < BasicObject
  def initialize(delegate, retries:, logger: nil)
    @delegate = delegate
    @max_retries = retries
    @logger = logger
  end

  def method_missing(method, *args, &block)
    retries = 0
    begin
      @delegate.send(method, *args, &block)
    rescue
      if retries < @max_retries
        retries += 1
        @logger.warn("#{@delegate.class}##{method} retry ##{retries} after error: #$!") if @logger
        retry
      else
        @logger.error("#{@delegate.class}##{method} failed after #{retries} retries") if @logger
        ::Kernel.raise
      end
    end
  end

  def respond_to_missing?(method, include_private = false)
    @delegate.respond_to?(method) || super
  end
end
