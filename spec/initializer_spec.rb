require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Initializer do
  it "should have an empty configuration" do
    expect(Initializer::configuration).to be_an_instance_of(Configuration)
  end
end

describe Initializer do
  before(:each) do
    Initializer::reset
  end

  it "run should yield the configuration object" do
    Initializer::run do |config|
      expect(config).to be_an_instance_of(Configuration)
    end
  end

  def make_dummy_configuration_change
    Initializer::run do |config|
      config.left = :dummy
    end
  end

  it "configuration should return the current configuration" do
    make_dummy_configuration_change
    expect(Initializer::configuration).to be_an_instance_of(Configuration)
    expect(Initializer::configuration.left).to eq(:dummy)
  end

  it "configuration= should set a new configuration" do
    make_dummy_configuration_change
    Initializer::configuration = :dummy_config
    expect(Initializer::configuration).to eq(:dummy_config)
  end

  it "reset should clear the configuration" do
    make_dummy_configuration_change
    Initializer::reset
    expect(Initializer::configuration.left).to eq({})
  end
end
