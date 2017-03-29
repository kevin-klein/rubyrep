require File.dirname(__FILE__) + '/spec_helper.rb'
require File.dirname(__FILE__) + "/../config/test_config.rb"

include RR

describe ReplicationDifference do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initialize should store the loaders" do
    session = Session.new
    loaders = LoggedChangeLoaders.new session
    diff = ReplicationDifference.new loaders
    expect(diff.loaders).to eq(loaders)
  end

  it "load should load left differences successfully" do
    session = Session.new
    begin
      session.left.execute('delete from rr_pending_changes')
      session.right.execute('delete from rr_pending_changes')

      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      expect(diff).to be_loaded
      expect(diff.type).to eq(:left)
      expect(diff.changes[:left].key).to eq({'id' => '1'})
    ensure
      session.left.execute('delete from rr_pending_changes')
    end
  end

  it "load should load right differences successfully" do
    session = Session.new
    begin
      session.right.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      expect(diff).to be_loaded
      expect(diff.type).to eq(:right)
      expect(diff.changes[:right].key).to eq({'id' => '1'})
    ensure
      session.right.execute('delete from rr_pending_changes')
    end
  end

  it "load should load conflict differences successfully" do
    config = deep_copy(standard_config)
    config.included_table_specs.clear
    config.include_tables /./

    session = Session.new config
    begin
      session.left.execute('delete from rr_pending_changes')
      session.right.execute('delete from rr_pending_changes')

      session.left.execute('delete from scanner_records')
      session.right.execute('delete from scanner_records')

      session.left.insert_record('scanner_records', {
          id: 2,
          name: 'Name1'
      })

      session.right.insert_record('scanner_records', {
          id: 2,
          name: 'Name2'
      })

      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id|2',
        'change_type' => 'U',
        'change_time' => Time.now
      }
      session.right.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id|2',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      expect(diff).to be_loaded
      expect(diff.type).to eq(:left)
      expect(diff.changes[:left].type).to eq(:insert)
      expect(diff.changes[:left].table).to eq('scanner_records')
      expect(diff.changes[:left].key).to eq({'id' => '2'})
      expect(diff.changes[:right].type).to eq(:no_change)
      expect(diff.changes[:right].table).to eq('scanner_records')
      expect(diff.changes[:right].key).to eq({'id' => '2'})
    ensure
      session.left.execute('delete from rr_pending_changes')
      session.right.execute('delete from rr_pending_changes')

      session.left.execute('delete from scanner_records')
      session.right.execute('delete from scanner_records')
    end
  end

  it "to_yaml should blank out session" do
    diff = ReplicationDifference.new :dummy_session
    expect(diff.to_yaml).not_to match(/session:/)
  end
end
