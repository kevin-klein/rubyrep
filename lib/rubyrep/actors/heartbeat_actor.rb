class HeartbeatActor
  include TActor

  def initialize(session)
    @session = session
  end

  def trigger_heartbeat
    $stdout.write "-" if @session.configuration.options[:replication_trace]

    RR.heartbeat(@session.configuration.options[:heartbeat_file])
  end

end
