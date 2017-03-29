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
end
