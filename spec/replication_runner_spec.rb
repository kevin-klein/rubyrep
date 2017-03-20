require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ReplicationRunner do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "should register itself with CommandRunner" do
    CommandRunner.commands['replicate'][:command].should == ReplicationRunner
    CommandRunner.commands['replicate'][:description].should be_an_instance_of(String)
  end

  it "process_options should make options as nil and teturn status as 1 if command line parameters are unknown" do
    # also verify that an error message is printed
    $stderr.should_receive(:puts).any_number_of_times
    runner = ReplicationRunner.new
    status = runner.process_options ["--nonsense"]
    runner.options.should == nil
    status.should == 1
  end

  it "process_options should make options as nil and return status as 1 if config option is not given" do
    # also verify that an error message is printed
    $stderr.should_receive(:puts).any_number_of_times
    runner = ReplicationRunner.new
    status = runner.process_options []
    runner.options.should == nil
    status.should == 1
  end

  it "process_options should make options as nil and return status as 0 if command line includes '--help'" do
    # also verify that the help message is printed
    $stderr.should_receive(:puts)
    runner = ReplicationRunner.new
    status = runner.process_options ["--help"]
    runner.options.should == nil
    status.should == 0
  end

  it "process_options should set the correct options" do
    runner = ReplicationRunner.new
    runner.process_options ["-c", "config_path"]
    runner.options[:config_file].should == 'config_path'
  end

  it "run should not start a replication if the command line is invalid" do
    $stderr.should_receive(:puts).any_number_of_times
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
    runner.session.should be_an_instance_of(Session)
    runner.session.should == runner.session # should only be created one time
  end

  it "pause_replication should not pause if next replication is already overdue" do
    runner = ReplicationRunner.new
    runner.stub!(:session).and_return(Session.new(standard_config))
    waiter_thread = mock('waiter_thread')
    waiter_thread.should_not_receive(:join)
    runner.instance_variable_set(:@waiter_thread, waiter_thread)

    runner.pause_replication # verify no wait during first run
    runner.instance_variable_set(:@last_run, 1.hour.ago)
    runner.pause_replication # verify no wait if overdue
  end

  it "pause_replication should pause for correct time frame" do
    runner = ReplicationRunner.new
    runner.stub!(:session).and_return(Session.new(deep_copy(standard_config)))
    runner.session.configuration.stub!(:options).and_return(:replication_interval => 2)
    waiter_thread = mock('waiter_thread')
    runner.instance_variable_set(:@waiter_thread, waiter_thread)

    now = Time.now
    Time.stub!(:now).and_return(now)
    runner.instance_variable_set(:@last_run, now - 1.seconds)
    waiter_thread.should_receive(:join).and_return {|time| time.to_f.should be_close(1.0, 0.01); 0}

    runner.pause_replication
  end

  it "prepare_replication should call ReplicationInitializer#prepare_replication" do
    runner = ReplicationRunner.new
    runner.stub!(:session).and_return(:dummy_session)
    initializer  = mock('replication_initializer')
    initializer.should_receive(:prepare_replication)
    ReplicationInitializer.should_receive(:new).with(:dummy_session).and_return(initializer)
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

    session.should_receive(:refresh).and_raise('bla')
    lambda {runner.execute_once}.should raise_error('bla')
    runner.instance_variable_get(:@session).should be_nil
  end

  it "execute_once should raise exception if replication run times out" do
    session = Session.new
    runner = ReplicationRunner.new
    runner.stub!(:session).and_return(session)

    terminated = mock("terminated")
    terminated.stub!(:terminated?).and_return(true)
    TaskSweeper.stub!(:timeout).and_return(terminated)

    lambda {runner.execute_once}.should raise_error(/timed out/)
  end
end
