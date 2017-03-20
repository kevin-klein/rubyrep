require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Replicators do
  before(:each) do
    @old_replicators = Replicators.replicators
  end

  after(:each) do
    Replicators.instance_variable_set :@replicators, @old_replicators
  end

  it "replicators should return empty hash if nil" do
    Replicators.instance_variable_set :@replicators, nil
    expect(Replicators.replicators).to eq({})
  end

  it "replicators should return the registered replicators" do
    Replicators.instance_variable_set :@replicators, :dummy_data
    expect(Replicators.replicators).to eq(:dummy_data)
  end

  it "configured_replicator should return the correct replicator" do
    options = {:replicator => :two_way}
    expect(Replicators.configured_replicator(options)).to eq(Replicators::TwoWayReplicator)
  end
  
  it "register should register the provided replicator" do
    Replicators.instance_variable_set :@replicators, nil
    Replicators.register :a_key => :a
    Replicators.register :b_key => :b
    expect(Replicators.replicators[:a_key]).to eq(:a)
    expect(Replicators.replicators[:b_key]).to eq(:b)
  end
end
