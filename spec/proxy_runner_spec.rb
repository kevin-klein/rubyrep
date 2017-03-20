require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ProxyRunner do
  before(:each) do
    allow(DRb).to receive(:start_service)
    allow(DRb.thread).to receive(:join)
    allow($stderr).to receive(:puts)
  end

  it "get_options should return options as nil and status as 1 if command line parameters are unknown" do
    # also verify that an error message is printed
    allow($stderr).to receive(:puts)
    options, status = ProxyRunner.new.get_options ["--nonsense"]
    expect(options).to eq(nil)
    expect(status).to eq(1)
  end

  it "get_options should return options as nil and status as 0 if command line includes '--help'" do
    # also verify that the help message is printed
    expect($stderr).to receive(:puts)
    options, status = ProxyRunner.new.get_options ["--help"]
    expect(options).to eq(nil)
    expect(status).to eq(0)
  end

  it "get_options should return the default options if none were given on the command line" do
    options, status = ProxyRunner.new.get_options []
    expect(options).to eq(ProxyRunner::DEFAULT_OPTIONS)
    expect(status).to eq(0)
  end

  it "get_options should return :host and :port options as per given command line" do
    options, status = ProxyRunner.new.get_options ["--host", "127.0.0.1", "--port", "1234"]
    expect(options).to eq({:host => '127.0.0.1', :port => 1234})
    expect(status).to eq(0)
  end

  it "construct_url should create the correct druby URL" do
    expect(ProxyRunner.new.build_url(:host => '127.0.0.1', :port => '1234')).to eq("druby://127.0.0.1:1234")
  end

  it "start_server should create a DatabaseProxy and start the DRB server" do
    expect(DatabaseProxy).to receive(:new)
    expect(DRb).to receive(:start_service).with("druby://127.0.0.1:1234", nil)
    allow(DRb).to receive(:thread).and_return(Object.new)
    expect(DRb.thread).to receive(:join)
    ProxyRunner.new.start_server("druby://127.0.0.1:1234")
  end

  it "run should not start a server if the command line is invalid" do
    expect(DRb).not_to receive(:start_service)
    allow(DRb).to receive(:thread).and_return(Object.new)
    expect(DRb.thread).not_to receive(:join)
    ProxyRunner.run("--nonsense")
  end

  it "run should start a server if the command line is correct" do
    expect(DRb).to receive(:start_service)
    allow(DRb).to receive(:thread).and_return(Object.new)
    expect(DRb.thread).to receive(:join)
    ProxyRunner.run(["--port=1234"])
  end

  it "should register itself with CommandRunner" do
    expect(CommandRunner.commands['proxy'][:command]).to eq(ProxyRunner)
    expect(CommandRunner.commands['proxy'][:description]).to be_an_instance_of(String)
  end
end
