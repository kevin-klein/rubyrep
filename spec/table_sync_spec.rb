require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TableSync do
  it "sync_options should return the correct table specific sync options" do
    config = deep_copy(standard_config)
    old_table_specific_options = config.tables_with_options
    begin
      config.options = {:syncer => :bla}
      config.include_tables 'scanner_records', {:syncer => :blub}
      expect(TableSync.new(Session.new(config), 'scanner_records').sync_options[:syncer]) \
        .to eq(:blub)
    ensure
      config.instance_eval {@tables_with_options = old_table_specific_options}
    end
  end

  it "execute_sync_hook should work if the hook is not configured" do
    session = Session.new standard_config
    sync = TableSync.new(session, 'scanner_records')
    sync.execute_sync_hook(:before_table_sync)
  end

  it "execute_sync_hook should execute the given SQL command" do
    config = deep_copy(standard_config)
    config.add_table_options 'scanner_records', :before_table_sync => 'dummy_command'
    session = Session.new config
    sync = TableSync.new(session, 'scanner_records')

    expect(session.left).to receive(:execute).with('dummy_command')
    expect(session.right).to receive(:execute).with('dummy_command')

    sync.execute_sync_hook(:before_table_sync)
  end

  it "execute_sync_hook should execute the given Proc" do
    config = deep_copy(standard_config)
    received_handler = nil
    config.add_table_options 'scanner_records',
      :before_table_sync => lambda {|helper| received_handler = helper}
    session = Session.new config
    sync = TableSync.new(session, 'scanner_records')
    sync.helper = :dummy_helper

    sync.execute_sync_hook(:before_table_sync)

    expect(received_handler).to eq(:dummy_helper)
  end

  it "event_filtered? should return false if there is no event filter" do
    session = Session.new standard_config
    sync = TableSync.new(session, 'scanner_records')

    expect(sync.event_filtered?(:left, 'id' => 1)).to be_falsey
  end

  it "event_filtered? should return false if event filter does not filter sync events" do
    config = deep_copy(standard_config)
    config.add_table_options 'scanner_records', :event_filter => Object.new
    session = Session.new config
    sync = TableSync.new(session, 'scanner_records')

    expect(sync.event_filtered?(:left, 'id' => 1)).to be_falsey
  end

  it "event_filtered? should signal filtering (i. e. return true) if the event filter result is false" do
    filter = Object.new
    def filter.before_sync(table, key, helper, type, row)
      false
    end
    config = deep_copy(standard_config)
    config.add_table_options 'scanner_records', :event_filter => filter
    session = Session.new config
    sync = TableSync.new(session, 'scanner_records')
    sync.helper = SyncHelper.new(sync)
    expect(sync.event_filtered?(:left, 'id' => 1)).to be_truthy
  end

  it "event_filtered? should return false if the event filter result is true" do
    filter = {}
    def filter.before_sync(table, key, helper, type, row)
      self[:args] = [table, key, helper, type, row]
      true
    end
    config = deep_copy(standard_config)
    config.add_table_options 'scanner_records', :event_filter => filter
    session = Session.new config
    sync = TableSync.new(session, 'scanner_records')
    sync.helper = SyncHelper.new(sync)
    expect(sync.event_filtered?(:left, 'id' => 1, 'name' => 'bla')).to be_falsey

    # verify correct parameter assignment
    expect(filter[:args]).to eq(['scanner_records', {'id' => 1}, sync.helper, :left, {'id' => 1, 'name' => 'bla'}])
  end

  it "run should synchronize the databases" do
    config = deep_copy(standard_config)
    config.options[:committer] = :never_commit
    config.options[:logged_sync_events] = [:all_conflicts]
    before_hook_called = false
    after_hook_called = false
    config.options[:before_table_sync] = lambda {|helper| before_hook_called = true}
    config.options[:after_table_sync] = lambda { |helper| after_hook_called = true}

    filter = Object.new
    def filter.before_sync(table, key, helper, type, row)
      key['id'] != 6
    end
    config.options[:event_filter] = filter
    session = Session.new(config)
    begin
      sync = TableSync.new(session, 'scanner_records')
      sync.run

      # Verify that sync events are logged
      row = session.left.select_one("select * from rr_logged_events where change_key = '2' order by id")
      expect(row['change_table']).to eq('scanner_records')
      expect(row['diff_type']).to eq('conflict')
      expect(row['description']).to eq('left_wins')

      # verify that the table was synchronized
      left_records = session.left.select_all("select * from scanner_records where id <> 6 order by id")
      right_records = session.right.select_all("select * from scanner_records where id <> 6 order by id")
      expect(left_records).to eq(right_records)

      # verify that the filtered out record was not synced
      expect(session.left.select_one("select * from scanner_records where id = 6")).
        to be_nil

      # verify that hooks where called
      expect(before_hook_called).to be_truthy
      expect(after_hook_called).to be_truthy
    ensure
      Committers::NeverCommitter.rollback_current_session
      session.left.execute "delete from rr_logged_events"
    end
  end

end
