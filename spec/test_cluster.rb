require 'docker/compose'
require 'pg'
require 'socket'

class TestCluster
  def initialize
    @compose = Docker::Compose.new

    # TODO this probably needs to change for boot2docker
    @host = 'localhost'
    # TODO probably need to detect this
    ENV['KAFKA_ADVERTISED_HOST_NAME'] = '172.17.0.1'
  end

  def start
    @compose.up(:kafka, :postgres, detached: true)

    pg_port = wait_for_port(:postgres, 5432, max_tries: 10) do |port|
      PG::Connection.ping(host: @host, port: port, user: 'postgres') == PG::PQPING_OK
    end
    @postgres = PG::Connection.open(host: @host, port: pg_port, user: 'postgres')
    @postgres.exec('CREATE EXTENSION IF NOT EXISTS bottledwater')

    @zookeeper_port = wait_for_port(:zookeeper, 2181) do |port|
      TCPSocket.open(@host, port).close
      true
    end

    @kafka_port = wait_for_port(:kafka, 9092) do |port|
      TCPSocket.open(@host, port).close
      true
    end

    @compose.up('bottledwater-json', detached: true)

    @state = :started
  end

  def started?
    @state == :started
  end

  def stopped?
    @state == :stopped
  end

  def postgres
    check_started!
    @postgres
  end

  def zookeeper_hostport
    check_started!
    "#{@host}:#{@zookeeper_port}"
  end

  def kafka_host
    check_started!
    @host
  end

  def kafka_port
    check_started!
    @kafka_port
  end

  def kafka_hostport
    "#{kafka_host}:#{kafka_port}"
  end

  def stop
    return unless started?

    postgres.close rescue nil

    @compose.stop
    @compose.run! :rm, f: true

    @state = :stopped
  end

  private
  def wait_for_port(service, port, max_tries: 5)
    mapped_hostport = @compose.port(service, port)
    _, mapped_port = mapped_hostport.split(':', 2)
    mapped_port = Integer(mapped_port)
    print "Waiting for #{service} on port #{mapped_port}..."
    tries = 0
    loop do
      tries += 1
      begin
        if yield mapped_port
          puts ' OK'
          break
        else
          print '.'
        end
      rescue
        print "not ready: #$! "
      end

      raise "#{service} not ready after #{max_tries} attempts" if tries >= max_tries

      sleep 1
    end

    mapped_port
  end

  def check_started!
    case @state
    when :started; return
    when nil; raise 'cluster not started'
    else; raise "cluster #{@state}"
    end
  end
end

TEST_CLUSTER = TestCluster.new

at_exit do
  TEST_CLUSTER.stop
end
