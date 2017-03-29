require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe LoggedChangeLoaders do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initializers should create both logged change loaders" do
    session = Session.new
    loaders = LoggedChangeLoaders.new(session)
    expect(loaders[:left].session).to eq(session)
    expect(loaders[:left].database).to eq(:left)
    expect(loaders[:right].database).to eq(:right)
  end

  it "update should execute a forced update of both logged change loaders" do
    session = Session.new
    loaders = LoggedChangeLoaders.new(session)
    expect(loaders[:left]).to receive(:update).with(:forced => true)
    expect(loaders[:right]).to receive(:update).with(:forced => true)
    loaders.update
  end

end

describe LoggedChangeLoader do
  before(:each) do
    Initializer.configuration = standard_config
  end

  # Note:
  # LoggedChangeLoader is a helper for LoggedChange.
  # It is tested through the specs for LoggedChange.

  it "oldest_change_time should return nil if there are no changes" do
    session = Session.new
    session.left.execute "delete from rr_pending_changes"
    loader = LoggedChangeLoader.new session, :left
    expect(loader.oldest_change_time).to be_nil
  end

end
