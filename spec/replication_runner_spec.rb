require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ReplicationRunner do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "should register itself with CommandRunner" do
    expect(CommandRunner.commands['replicate'][:command]).to eq(ReplicationRunner)
    expect(CommandRunner.commands['replicate'][:description]).to be_an_instance_of(String)
  end

  it "process_options should make options as nil and teturn status as 1 if command line parameters are unknown" do
    # also verify that an error message is printed
    allow($stderr).to receive(:puts)
    runner = ReplicationRunner.new
    status = runner.process_options ["--nonsense"]
    expect(runner.options).to eq(nil)
    expect(status).to eq(1)
  end

  it "process_options should make options as nil and return status as 1 if config option is not given" do
    # also verify that an error message is printed
    allow($stderr).to receive(:puts)
    runner = ReplicationRunner.new
    status = runner.process_options []
    expect(runner.options).to eq(nil)
    expect(status).to eq(1)
  end

  it "process_options should make options as nil and return status as 0 if command line includes '--help'" do
    # also verify that the help message is printed
    expect($stderr).to receive(:puts)
    runner = ReplicationRunner.new
    status = runner.process_options ["--help"]
    expect(runner.options).to eq(nil)
    expect(status).to eq(0)
  end

  it "process_options should set the correct options" do
    runner = ReplicationRunner.new
    runner.process_options ["-c", "config_path"]
    expect(runner.options[:config_file]).to eq('config_path')
  end

  it "run should not start a replication if the command line is invalid" do
    allow($stderr).to receive(:puts)
    ReplicationRunner.any_instance_should_not_receive(:execute) {
      ReplicationRunner.run(["--nonsense"])
    }
  end

  it "run should start a replication if the command line is correct" do
    ReplicationRunner.any_instance_should_receive(:execute) {
      ReplicationRunner.run(["--config=path"])
    }
  end

  it "session should create and return the session" do
    runner = ReplicationRunner.new
    runner.options = {:config_file => "config/test_config.rb"}
    expect(runner.session).to be_an_instance_of(Session)
    expect(runner.session).to eq(runner.session) # should only be created one time
  end

  it "pause_replication should not pause if next replication is already overdue" do
    runner = ReplicationRunner.new
    allow(runner).to receive(:session).and_return(Session.new(standard_config))
    waiter_thread = double('waiter_thread')
    expect(waiter_thread).not_to receive(:join)
    runner.instance_variable_set(:@waiter_thread, waiter_thread)

    runner.pause_replication # verify no wait during first run
    runner.instance_variable_set(:@last_run, 1.hour.ago)
    runner.pause_replication # verify no wait if overdue
  end

  it "pause_replication should pause for correct time frame" do
    runner = ReplicationRunner.new
    allow(runner).to receive(:session).and_return(Session.new(deep_copy(standard_config)))
    allow(runner.session.configuration).to receive(:options).and_return(:replication_interval => 2)
    waiter_thread = double('waiter_thread')
    runner.instance_variable_set(:@waiter_thread, waiter_thread)

    now = Time.now
    allow(Time).to receive(:now).and_return(now)
    runner.instance_variable_set(:@last_run, now - 1.seconds)
    expect(waiter_thread).to receive(:join) {|time| expect(time.to_f).to be_within(0.01).of(1.0); 0}

    runner.pause_replication
  end

  it "prepare_replication should call ReplicationInitializer#prepare_replication" do
    runner = ReplicationRunner.new
    allow(runner).to receive(:session).and_return(:dummy_session)
    initializer  = double('replication_initializer')
    expect(initializer).to receive(:prepare_replication)
    expect(ReplicationInitializer).to receive(:new).with(:dummy_session).and_return(initializer)
    runner.prepare_replication
  end

  # Checks a specified number of times with specified waiting period between
  # attempts if a given SQL query returns records.
  # Returns +true+ if a record was found
  # * +session+: an active Session object
  # * +database+: either :+left+ or :+right+
  # * +query+: sql query to execute
  # * +max_attempts+: number of attempts to find the record
  # * +interval+: waiting time in seconds between attempts
  def check_for_record(session, database, query, max_attempts, interval)
    found = false

    max_attempts.times do
      found = !!session.send(database).select_one(query)
      break if found
      sleep interval
    end

    found
  end

  it "execute_once should clean up after failed replication runs" do
    runner = ReplicationRunner.new
    session = Session.new
    runner.instance_variable_set(:@session, session)

    expect(session).to receive(:refresh).and_raise('bla')
    expect {runner.execute_once}.to raise_error('bla')
    expect(runner.instance_variable_get(:@session)).to be_nil
  end

  it "execute_once should raise exception if replication run times out" do
    session = Session.new
    runner = ReplicationRunner.new
    allow(runner).to receive(:session).and_return(session)

    terminated = double("terminated")
    allow(terminated).to receive(:terminated?).and_return(true)
    allow(TaskSweeper).to receive(:timeout).and_return(terminated)

    expect {runner.execute_once}.to raise_error(/timed out/)
  end
end
