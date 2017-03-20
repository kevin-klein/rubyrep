require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

class MyLogHelper
  include LogHelper
end

describe LogHelper do
  before(:each) do
  end

  it "should do nothing if the description fields are small enough" do
    expect(MyLogHelper.new.fit_description_columns("bla", "blub")).
      to eq(%w(bla blub))
  end

  it "should cut details to fit into the 'long_description' column" do
    expect(MyLogHelper.new.fit_description_columns(
      "bla",
      "x" * (ReplicationInitializer::LONG_DESCRIPTION_SIZE - 1) + "yz")).
      to eq(["bla", "x" * (ReplicationInitializer::LONG_DESCRIPTION_SIZE - 1) + "y"])
  end

  it "should cut outcome to fit into the 'description' column" do
    expect(MyLogHelper.new.fit_description_columns(
      "x" * (ReplicationInitializer::DESCRIPTION_SIZE - 1) + "yz",
      "blub")[0]).
      to eq("x" * (ReplicationInitializer::DESCRIPTION_SIZE - 1) + "y")
  end

  it "should carry over a long outcome into the 'long_description' column" do
    expect(MyLogHelper.new.fit_description_columns(
      "x" * (ReplicationInitializer::DESCRIPTION_SIZE - 1) + "yz",
      "blub")[1]).
      to eq("x" * (ReplicationInitializer::DESCRIPTION_SIZE - 1) + "yz\nblub")
  end

end