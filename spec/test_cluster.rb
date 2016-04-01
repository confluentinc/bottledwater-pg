require 'backticks'
require 'docker'
require 'docker/compose'
require 'kazoo'
require 'logger'
require 'pg'
require 'schema_registry'
require 'set'
require 'socket'

require 'retrying_proxy'


class TestCluster
  POSTGRES_EXTENSIONS = %w(
    bottledwater
    hstore
  ).freeze

  def initialize
    @logger = Logger.new($stderr)

    # override Docker::Compose's default interactive: true
    runner = Backticks::Runner.new(interactive: false)
    compose = Docker::Compose::Session.new(runner)
    @compose = RetryingProxy.new(compose, retries: 4, logger: @logger)

    @docker = RetryingProxy.new(Docker.new, retries: 4, logger: @logger)

    # TODO this probably needs to change for boot2docker
    @host = 'localhost'

    reset
  end

  def reset
    self.kafka_log_cleanup_policy = :compact
    self.kafka_auto_create_topics_enable = true

    self.bottledwater_format = :json
  end

  def start(without: [])
    @state = :starting

    raise "cluster already #{@state}!" if started?

    @started_without = Set.new(without)

    self.kafka_advertised_host_name = detect_docker_host_ip

    start_service(:zookeeper, :kafka, :postgres)

    pg_port = wait_for_port(:postgres, 5432, max_tries: 10) do |port|
      PG::Connection.ping(host: @host, port: port, user: 'postgres') == PG::PQPING_OK
    end
    @postgres = PG::Connection.open(host: @host, port: pg_port, user: 'postgres')
    POSTGRES_EXTENSIONS.each do |extension|
      @postgres.exec("CREATE EXTENSION IF NOT EXISTS #{extension}")
    end

    unless @started_without.include?(:kafka)
      @zookeeper_port = wait_for_tcp_port(:zookeeper, 2181)
      @kazoo = Kazoo::Cluster.new("#{@host}:#{@zookeeper_port}")

      @kafka_port = wait_for_tcp_port(:kafka, 9092)
    end

    if schema_registry_needed?
      start_service(:'schema-registry')
      schema_registry = nil
      @schema_registry_port = wait_for_port(:'schema-registry', 8081, max_tries: 10) do |port|
        schema_registry = SchemaRegistry::Client.new("http://#{@host}:#{port}")
        schema_registry.subjects rescue nil
      end
      @schema_registry = schema_registry
    end

    start_service(bottledwater_service)
    wait_for_container(bottledwater_service)

    @logger << 'Letting things settle'
    5.times do
      @logger << '.'
      sleep 1
    end
    @logger << " OK\n"

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

  def kafka_log_cleanup_policy=(policy)
    ENV['KAFKA_LOG_CLEANUP_POLICY'] = policy.to_s
  end

  def kafka_auto_create_topics_enable=(enabled)
    ENV['KAFKA_AUTO_CREATE_TOPICS_ENABLE'] = enabled.to_s
  end

  attr_accessor :bottledwater_format

  def bottledwater_service
    :"bottledwater-#{bottledwater_format}"
  end

  def schema_registry_needed?
    bottledwater_format == :avro && !@started_without.include?(:'schema-registry')
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

  def schema_registry_url
    check_started!
    "http://#{@host}:#{@schema_registry_port}"
  end

  def healthy?
    postgres_running? && bottledwater_running?
  end

  def postgres_running?
    service_running?(:postgres)
  end

  def bottledwater_running?
    service_running?(bottledwater_service)
  end

  def stop(should_reset: true, dump_logs: true)
    return if stopped?

    kazoo.close rescue nil
    postgres.close rescue nil

    failed_services.each {|container| dump_container_logs(container) } if dump_logs

    @compose.stop
    @compose.run! :rm, f: true, v: true

    reset if should_reset

    @state = :stopped
  end

  def restart(**kwargs)
    stop(should_reset: false, **kwargs)
    start
  end

  private
  def detect_docker_host_ip
    ip_output = @docker.run!(:run, '--rm', 'debian:latest', 'ip', 'route').split("\n")

    gateway_line = ip_output.detect {|line| line =~ /^default via (.*) dev / }
    raise "Unexpected output from `ip route`: #{ip_output}" unless gateway_line

    @docker_host_ip = $1
    @logger.info "Detected Docker host IP as #{@docker_host_ip}"
    @docker_host_ip
  end

  def start_service(*services)
    services_to_start = services.reject {|service| @started_without.include?(service) }
    return if services_to_start.empty?
    @compose.up(*services_to_start, detached: true, no_deps: true)
  end

  def service_running?(service)
    container_for_service(service).to_h.fetch('State').fetch('Running')
  end

  def wait_for_tcp_port(service, port, max_tries: 5)
    wait_for_port(service, port) do |mapped_port|
      TCPSocket.open(@host, mapped_port).close
      true
    end
  end

  def wait_for_port(service, port, max_tries: 5)
    if starting? && @started_without.include?(service)
      raise "Waiting for #{service} when we deliberately started without it!"
    end

    mapped_hostport = @compose.port(service, port)
    _, mapped_port = mapped_hostport.split(':', 2)
    mapped_port = Integer(mapped_port)

    wait_for(service, message: "#{service} on port #{mapped_port}", max_tries: max_tries) do
      if yield mapped_port
        mapped_port
      else
        nil
      end
    end
  end

  def wait_for_container(service, max_tries: 5)
    wait_for(service, max_tries: max_tries) do
      container = container_for_service(service)
      if container && container.to_h.fetch('State').fetch('Running')
        container
      else
        nil
      end
    end
  end

  def wait_for(service, message: service, max_tries:)
    if starting? && @started_without.include?(service)
      raise "Waiting for #{service} when we deliberately started without it!"
    end

    @logger << "Waiting for #{message}..."
    tries = 0
    result = nil
    loop do
      sleep 1

      tries += 1
      begin
        result = yield
        if result
          @logger << " OK\n"
          break
        else
          @logger << '.'
        end
      rescue
        @logger << "not ready: #$! "
      end

      raise "#{service} not ready after #{max_tries} attempts" if tries >= max_tries
    end

    result
  end

  def container_for_service(service)
    check_started! unless starting?
    id_output = @compose.run!(:ps, {q: true}, service)
    return nil if id_output.nil?
    @docker.inspect(id_output.strip)
  end

  def starting?
    @state == :starting
  end

  def check_started!
    case @state
    when :started; return
    when nil; raise 'cluster not started'
    else; raise "cluster #{@state}"
    end
  end

  def failed_services
    ps_output = @compose.run!(:ps).
      split("\n").
      drop(2) # header rows
    container_names = ps_output.map {|line| line.strip.split.first }
    containers = container_names.map {|name| @docker.inspect(name) }
    containers.select {|container| container.exit_code != 0 }
  end

  def dump_container_logs(container)
    logs_command = @docker.shell.run(:docker, :logs, container.id).join
    unless logs_command.status.success?
      @logger.warn "Failed to capture logs for container #{container.name} (exit code #{container.exit_code})"
      return
    end
    stdout = logs_command.captured_output
    stderr = logs_command.captured_error
    unless stdout.strip.empty?
      @logger << "Stdout from container #{container.name} (exit code #{container.exit_code})\n"
      @logger << ('-' * 80 + "\n")
      @logger << stdout
      @logger << "\n"
      @logger << ('-' * 80 + "\n")
    end
    unless stderr.strip.empty?
      @logger << "Stderr from container #{container.name} (exit code #{container.exit_code})\n"
      @logger << ('-' * 80 + "\n")
      @logger << stderr
      @logger << "\n"
      @logger << ('-' * 80 + "\n")
    end
  end
end

TEST_CLUSTER = TestCluster.new

at_exit do
  TEST_CLUSTER.stop
end
