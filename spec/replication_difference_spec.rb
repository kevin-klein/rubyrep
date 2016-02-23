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
    diff.loaders.should == loaders
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

      diff.should be_loaded
      diff.type.should == :left
      diff.changes[:left].key.should == {'id' => '1'}
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

      diff.should be_loaded
      diff.type.should == :right
      diff.changes[:right].key.should == {'id' => '1'}
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

      diff.should be_loaded
      diff.type.should == :conflict
      diff.changes[:left].type.should == :update
      diff.changes[:left].table.should == 'scanner_records'
      diff.changes[:left].key.should == {'id' => '2'}
      diff.changes[:right].type.should == :delete
      diff.changes[:right].table.should == 'scanner_records'
      diff.changes[:right].key.should == {'id' => '2'}
    ensure
      session.left.execute('delete from rr_pending_changes')
      session.right.execute('delete from rr_pending_changes')

      session.left.execute('delete from scanner_records')
      session.right.execute('delete from scanner_records')
    end
  end

  it "amend should amend the replication difference with new found changes" do
    session = Session.new
    begin
      session.right.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      diff = ReplicationDifference.new LoggedChangeLoaders.new(session)
      diff.load

      diff.should be_loaded
      diff.type.should == :right
      diff.changes[:right].key.should == {'id' => '1'}

      # if there are no changes, the diff should still be the same
      diff.amend
      diff.type.should == :right
      diff.changes[:right].key.should == {'id' => '1'}

      # should recognize new changes
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'scanner_records',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      diff.amend
      diff.type.should == :conflict
      diff.changes[:left].key.should == {'id' => '1'}
      diff.changes[:right].key.should == {'id' => '1'}
    ensure
      session.left.execute('delete from rr_pending_changes')
      session.right.execute('delete from rr_pending_changes')
    end
  end

  it "to_yaml should blank out session" do
    diff = ReplicationDifference.new :dummy_session
    diff.to_yaml.should_not =~ /session:/
  end
end
