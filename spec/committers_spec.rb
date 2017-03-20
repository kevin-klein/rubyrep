require File.dirname(__FILE__) + '/spec_helper.rb'
require File.dirname(__FILE__) + "/../config/test_config.rb"

include RR

shared_examples_for 'Committer' do
  before(:each) do
    @old_committers = Committers.committers
  end

  after(:each) do
    Committers.instance_variable_set :@committers, @old_committers
  end

  it "committers should return empty hash if nil" do
    Committers.instance_variable_set :@committers, nil
    expect(Committers.committers).to eq({})
  end

  it "committers should return the registered committers" do
    Committers.instance_variable_set :@committers, :dummy_data
    expect(Committers.committers).to eq(:dummy_data)
  end

  it "register should register the provided commiter" do
    Committers.instance_variable_set :@committers, nil
    Committers.register :a_key => :a
    Committers.register :b_key => :b
    expect(Committers.committers[:a_key]).to eq(:a)
    expect(Committers.committers[:b_key]).to eq(:b)
  end
end


shared_examples_for "Committer" do
  it "should support the right constructor interface" do
    session = double("session")
    allow(session).to receive(:left) \
      .and_return(double("left connection", :null_object => true))
    allow(session).to receive(:right) \
      .and_return(double("right connection", :null_object => true))
    @committer.class.new session
  end

  it "should proxy insert_record, update_record and delete_record calls" do
    left_connection = double("left connection", :null_object => true)
    expect(left_connection).to receive(:insert_record).with("left", :dummy_insert_values)

    right_connection = double("right connection", :null_object => true)
    expect(right_connection).to receive(:update_record).with("right", :dummy_update_values, :dummy_org_key)
    expect(right_connection).to receive(:delete_record).with("right", :dummy_delete_values)

    session = double("session")
    allow(session).to receive(:left).and_return(left_connection)
    allow(session).to receive(:right).and_return(right_connection)

    committer = @committer.class.new session

    committer.insert_record :left, 'left', :dummy_insert_values
    committer.update_record :right, 'right', :dummy_update_values, :dummy_org_key
    committer.delete_record :right, 'right', :dummy_delete_values
  end

  it "should support finalize" do
    @committer.finalize(false)
  end
end

describe Committers::DefaultCommitter do
  before(:each) do
    @session = double("session")
    allow(@session).to receive(:left).and_return(:left_connection)
    allow(@session).to receive(:right).and_return(:right_connection)
    @committer = Committers::DefaultCommitter.new @session
  end

  it "should register itself" do
    expect(Committers.committers[:default]).to eq(Committers::DefaultCommitter)
  end

  it "initialize should store the provided parameters" do
    expect(@committer.session).to eq(@session)
    expect(@committer.connections) \
      .to eq({:left => @session.left, :right => @session.right})
  end

  it "new_transaction? should return false" do
    expect(@committer.new_transaction?).to be_falsey
  end

  it_should_behave_like "Committer"
end
