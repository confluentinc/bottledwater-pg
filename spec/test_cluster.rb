require 'docker'
require 'docker/compose'
require 'kazoo'
require 'pg'
require 'socket'

class TestCluster
  def initialize
    @compose = Docker::Compose.new
    @docker = Docker.new

    # TODO this probably needs to change for boot2docker
    @host = 'localhost'

    reset
  end

  def reset
    # TODO probably need to detect this
    self.kafka_advertised_host_name = '172.17.0.1'

    self.kafka_auto_create_topics_enable = true
  end

  def start
    raise "cluster already #{state}!" if started?

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
    @kazoo = Kazoo::Cluster.new("#{@host}:#{@zookeeper_port}")

    @kafka_port = wait_for_port(:kafka, 9092) do |port|
      TCPSocket.open(@host, port).close
      true
    end

    @compose.up('bottledwater-json', detached: true)
    sleep 1

    @state = :started
  end

  def started?
    @state == :started
  end

  def stopped?
    @state == :stopped
  end

  def kafka_advertised_host_name=(hostname)
    ENV['KAFKA_ADVERTISED_HOST_NAME'] = hostname
  end

  def kafka_auto_create_topics_enable=(enabled)
    ENV['KAFKA_AUTO_CREATE_TOPICS_ENABLE'] = enabled.to_s
  end

  def postgres
    check_started!
    @postgres
  end

  def zookeeper_hostport
    check_started!
    "#{@host}:#{@zookeeper_port}"
  end

  def kazoo
    check_started!
    @kazoo
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

  def bottledwater_running?
    container_for_service('bottledwater-json').status == 'running'
  end

  def stop
    return unless started?

    kazoo.close rescue nil
    postgres.close rescue nil

    @compose.stop
    @compose.run! :rm, f: true

    reset

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

  def container_for_service(service)
    check_started!
    id_output = @compose.run!(:ps, {q: true}, service)
    return nil if id_output.nil?
    @docker.inspect(id_output.strip)
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
