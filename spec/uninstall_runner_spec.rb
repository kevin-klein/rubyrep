require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe UninstallRunner do
  before(:each) do
  end

  it "should register itself with CommandRunner" do
    expect(CommandRunner.commands['uninstall'][:command]).to eq(UninstallRunner)
    expect(CommandRunner.commands['uninstall'][:description]).to be_an_instance_of(String)
  end

  it "process_options should make options as nil and teturn status as 1 if command line parameters are unknown" do
    # also verify that an error message is printed
    allow($stderr).to receive(:puts)
    runner = UninstallRunner.new
    status = runner.process_options ["--nonsense"]
    expect(runner.options).to eq(nil)
    expect(status).to eq(1)
  end

  it "process_options should make options as nil and return status as 1 if config option is not given" do
    # also verify that an error message is printed
    allow($stderr).to receive(:puts)
    runner = UninstallRunner.new
    status = runner.process_options []
    expect(runner.options).to eq(nil)
    expect(status).to eq(1)
  end

  it "process_options should make options as nil and return status as 0 if command line includes '--help'" do
    # also verify that the help message is printed
    expect($stderr).to receive(:puts)
    runner = UninstallRunner.new
    status = runner.process_options ["--help"]
    expect(runner.options).to eq(nil)
    expect(status).to eq(0)
  end

  it "process_options should set the correct options" do
    runner = UninstallRunner.new
    runner.process_options ["-c", "config_path"]
    expect(runner.options[:config_file]).to eq('config_path')
  end

  it "run should not start an uninstall if the command line is invalid" do
    allow($stderr).to receive(:puts)
    UninstallRunner.any_instance_should_not_receive(:execute) {
      UninstallRunner.run(["--nonsense"])
    }
  end

  it "run should start an uninstall if the command line is correct" do
    UninstallRunner.any_instance_should_receive(:execute) {
      UninstallRunner.run(["--config=path"])
    }
  end

  it "session should create and return the session" do
    runner = UninstallRunner.new
    runner.options = {:config_file => "config/test_config.rb"}
    expect(runner.session).to be_an_instance_of(Session)
    expect(runner.session).to eq(runner.session) # should only be created one time
  end

  it "execute should uninstall all rubyrep elements" do
    begin
      org_stdout, $stdout = $stdout, StringIO.new
      config = deep_copy(standard_config)
      config.options[:rep_prefix] = 'rx'
      session = Session.new(config)
      initializer = ReplicationInitializer.new(session)

      initializer.ensure_infrastructure
      initializer.create_trigger :left, 'scanner_records'

      runner = UninstallRunner.new
      allow(runner).to receive(:session).and_return(session)

      runner.execute

      expect(initializer.trigger_exists?(:left, 'scanner_records')).to be_falsey
      expect(initializer.change_log_exists?(:left)).to be_falsey
      expect(session.right.tables.include?('rx_running_flags')).to be_falsey
      expect(initializer.event_log_exists?).to be_falsey

      $stdout.string =~ /uninstall completed/i
    ensure
      $stdout = org_stdout
    end
  end
end