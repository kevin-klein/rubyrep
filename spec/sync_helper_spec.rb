require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe SyncHelper do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initialize should initialize the correct committer" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    c = helper.instance_eval {committer}
    expect(c).to be_an_instance_of(Committers::DefaultCommitter)
    expect(c.session).to eq(helper.session)
  end

  it "session should return the session" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    expect(helper.session).to eq(sync.session)
  end

  it "ensure_event_log should ask the replication_initializer to ensure the event log" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    ReplicationInitializer.any_instance_should_receive(:ensure_event_log) do
      helper.ensure_event_log
    end
  end

  it "log_sync_outcome should log the replication outcome correctly" do
    session = Session.new
    begin
      sync = TableSync.new(Session.new, 'scanner_records')
      helper = SyncHelper.new(sync)

      # Verify that the log information are made fitting
      expect(helper).to receive(:fit_description_columns).
        with('my_outcome', 'my_long_description').
        and_return(['my_outcomeX', 'my_long_descriptionY'])

      helper.log_sync_outcome(
        {'bla' => 'blub', 'id' => 1},
        'my_sync_type',
        'my_outcome',
        'my_long_description'
      )

      row = session.left.select_one("select * from rr_logged_events order by id desc")
      expect(row['activity']).to eq('sync')
      expect(row['change_table']).to eq('scanner_records')
      expect(row['diff_type']).to eq('my_sync_type')
      expect(row['change_key']).to eq('1')
      expect(row['left_change_type']).to be_nil
      expect(row['right_change_type']).to be_nil
      expect(row['description']).to eq('my_outcomeX')
      expect(row['long_description']).to eq('my_long_descriptionY')
      expect(Time.parse(row['event_time'])).to be <= 10.seconds.ago
      expect(row['diff_dump']).to eq(nil)
    ensure
      session.left.execute('delete from rr_logged_events')
    end
  end

  it "log_sync_outcome should log events for combined primary key tables correctly" do
    session = Session.new
    begin
      sync = TableSync.new(Session.new, 'extender_combined_key')
      helper = SyncHelper.new(sync)

      helper.log_sync_outcome(
        {'bla' => 'blub', 'first_id' => 1, 'second_id' => 2},
        'my_sync_type',
        'my_outcome',
        'my_long_description'
      )

      row = session.left.select_one("select * from rr_logged_events order by id desc")
    ensure
      session.left.execute('delete from rr_logged_events')
    end
  end

  it "left_table and right_table should return the correct table names" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    expect(helper.left_table).to eq('scanner_records')
    expect(helper.right_table).to eq('scanner_records')

    sync = TableSync.new(Session.new, 'scanner_records', 'right_table')
    helper = SyncHelper.new(sync)
    expect(helper.left_table).to eq('scanner_records')
    expect(helper.right_table).to eq('right_table')
  end

  it "tables should return the correct table name hash" do
    sync = TableSync.new(Session.new, 'scanner_records', 'right_table')
    helper = SyncHelper.new(sync)
    expect(helper.tables).to eq({:left => 'scanner_records', :right => 'right_table'})
  end

  it "table_sync should return the current table sync instance" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    expect(helper.table_sync).to eq(sync)
  end

  it "sync_options should return the correct sync options" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    expect(helper.sync_options).to eq(sync.sync_options)
  end

  it "insert_record should insert the given record" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    c = helper.instance_eval {committer}
    expect(c).to receive(:insert_record).with(:right, 'scanner_records', :dummy_record)
    helper.insert_record :right, 'scanner_records', :dummy_record
  end

  it "update_record should update the given record" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    c = helper.instance_eval {committer}
    expect(c).to receive(:update_record).with(:right, 'scanner_records', :dummy_record, nil)
    helper.update_record :right, 'scaner_records', :dummy_record
  end

  it "update_record should update the given record with the provided old key" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    c = helper.instance_eval {committer}
    expect(c).to receive(:update_record).with(:right, 'scanner_records', :dummy_record, :old_key)
    helper.update_record :right, 'scanner_records', :dummy_record, :old_key
  end

  it "delete_record should delete the given record" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    c = helper.instance_eval {committer}
    expect(c).to receive(:delete_record).with(:right, 'scanner_records', :dummy_record)
    helper.delete_record :right, 'scanner_records', :dummy_record
  end

  it "finalize should be delegated to the committer" do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)

    # finalize itself should not lead to creation of committer
    helper.finalize
    expect(helper.instance_eval {@committer}).to be_nil

    c = helper.instance_eval {committer}
    expect(c).to receive(:finalize).with(false)
    helper.finalize(false)
  end
end
