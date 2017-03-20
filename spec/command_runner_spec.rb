require File.dirname(__FILE__) + '/spec_helper.rb'
require File.dirname(__FILE__) + "/../config/test_config.rb"

include RR

describe CommandRunner do
  before(:each) do
    @org_commands = CommandRunner.commands
    CommandRunner.instance_variable_set :@commands, nil
  end

  after(:each) do
    CommandRunner.instance_variable_set :@commands, @org_commands
  end

  it "show_version should print the version string" do
    expect($stdout).to receive(:puts).with(/rubyrep version ([0-9]+\.){2}[0-9]+/)
    CommandRunner.show_version
  end

  it "register should register commands, commands should return it" do
    CommandRunner.register :bla => :bla_command
    CommandRunner.register :blub => :blub_command
    expect(CommandRunner.commands).to eq({
      :bla => :bla_command,
      :blub => :blub_command
    })
  end

  it "run should print a short help if --help is specified" do
    org_stderr = $stderr
    $stderr = StringIO.new
    begin
      CommandRunner.register 'c1' => {:description => 'desc 1'}, 'c2' => {:description => 'desc 2'}
      CommandRunner.run(['--help'])
      expect($stderr.string).to match(/Usage/)
      expect($stderr.string).to match(/c1.*desc 1\n/)
      expect($stderr.string).to match(/c2.*desc 2\n/)
    ensure
      $stderr = org_stderr
    end
  end

  it "run should print help if no command line parameters are given" do
    org_stderr = $stderr
    $stderr = StringIO.new
    begin
      expect(CommandRunner.run([])).to eq(1)
      expect($stderr.string).to match(/Available commands/)
    ensure
      $stderr = org_stderr
    end
  end

  it "run should print help if --help or help without further params is given" do
    org_stderr = $stderr
    $stderr = StringIO.new
    begin
      expect(CommandRunner.run(['--help'])).to eq(0)
      expect($stderr.string).to match(/Available commands/)
      $stderr = StringIO.new
      expect(CommandRunner.run(['help'])).to eq(0)
      expect($stderr.string).to match(/Available commands/)
    ensure
      $stderr = org_stderr
    end
  end

  it "run should print version if --version is given" do
    expect(CommandRunner).to receive(:show_version)
    CommandRunner.run(['--version'])
  end

  it "run should call the specified command with the specified params" do
    c = double('dummy_command')
    expect(c).to receive(:run).with(['param1', 'param2'])
    CommandRunner.register 'dummy_command' => {:command => c}
    CommandRunner.run(['dummy_command', 'param1', 'param2'])
  end

  it "run should print help if unknown command is given" do
    org_stderr = $stderr
    $stderr = StringIO.new
    begin
      expect(CommandRunner.run('non-existing-command')).to eq(1)
      expect($stderr.string).to match(/Available commands/)
    ensure
      $stderr = org_stderr
    end
  end

  it "run should print stacktrace if --verbose option is given" do
    org_stderr = $stderr
    $stderr = StringIO.new
    begin
      c = double('dummy_command')
      allow(c).to receive(:run).and_raise('bla')
      CommandRunner.register 'dummy_command' => {:command => c}
      expect(CommandRunner.run(['--verbose', 'dummy_command', '-c', 'non_existing_file'])).to eq(1)
      expect($stderr.string).to match(/Exception caught/)
      expect($stderr.string).to match(/command_runner.rb:[0-9]+:in /)

      # also verify that no stacktrace is printed if --verbose is not specified
      $stderr = StringIO.new
      expect(CommandRunner.run(['dummy_command', '-c', 'non_existing_file'])).to eq(1)
      expect($stderr.string).to match(/Exception caught/)
      expect($stderr.string).not_to match(/command_runner.rb:[0-9]+:in /)
    ensure
      $stderr = org_stderr
    end
  end
end

describe HelpRunner do
  it "should register itself" do
    expect(CommandRunner.commands['help'][:command]).to eq(HelpRunner)
    expect(CommandRunner.commands['help'][:description]).to be_an_instance_of(String)
  end

  it "run should call help for the specified command" do
    expect(CommandRunner).to receive(:run).with(['dummy_command', '--help'])
    HelpRunner.run(['dummy_command'])
  end

  it "run should print help for itself if '--help' or 'help' is specified" do
    org_stderr = $stderr
    $stderr = StringIO.new
    begin
      HelpRunner.run(['--help'])
      expect($stderr.string).to match(/Shows the help for the specified command/)

      $stderr = StringIO.new
      HelpRunner.run(['help'])
      expect($stderr.string).to match(/Shows the help for the specified command/)
    ensure
      $stderr = org_stderr
    end
  end
end
