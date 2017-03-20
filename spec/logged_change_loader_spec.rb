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

  it "oldest_change_time should return the time of the oldest change" do
    session = Session.new
    begin
      time = Time.now
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => time
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'left_table',
        'change_key' => 'id|2',
        'change_type' => 'I',
        'change_time' => 100.seconds.from_now
      }
      loader = LoggedChangeLoader.new session, :left
      expect(loader.oldest_change_time).to.to_s == time.to_s
    ensure
      session.left.connection.execute('delete from rr_pending_changes')
    end
  end

end
