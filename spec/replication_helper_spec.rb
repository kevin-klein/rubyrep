require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ReplicationHelper do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it 'initialize should initialize the correct committer' do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    c = helper.instance_eval { @committer }
    expect(c).to be_an_instance_of(Committers::DefaultCommitter)
    expect(c.session).to eq(helper.session)
  end

  it 'session should return the session' do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    expect(helper.session).to eq(rep_run.session)
  end

  it 'new_transaction? should delegate to the committer' do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    c = helper.instance_eval { @committer }
    expect(c).to receive(:new_transaction?).and_return(true)
    expect(helper.new_transaction?).to be_truthy
  end

  it 'replication_run should return the current ReplicationRun instance' do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    expect(helper.replication_run).to eq(rep_run)
  end

  it 'options should return the correct options' do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    expect(helper.options).to eq(session.configuration.options)
  end

  it 'insert_record should insert the given record' do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    c = helper.instance_eval { committer }
    expect(c).to receive(:insert_record).with(:right, 'scanner_records', :dummy_record)
    helper.insert_record :right, 'scanner_records', :dummy_record
  end

  it 'update_record should update the given record' do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    c = helper.instance_eval { committer }
    expect(c).to receive(:update_record).with(:right, 'scanner_records', :dummy_record, nil)
    helper.update_record :right, 'scanner_records', :dummy_record
  end

  it 'update_record should update the given record with the provided old key' do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    c = helper.instance_eval { committer }
    expect(c).to receive(:update_record).with(:right, 'scanner_records', :dummy_record, :old_key)
    helper.update_record :right, 'scanner_records', :dummy_record, :old_key
  end

  it 'delete_record should delete the given record' do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    c = helper.instance_eval { committer }
    expect(c).to receive(:delete_record).with(:right, 'scanner_records', :dummy_record)
    helper.delete_record :right, 'scanner_records', :dummy_record
  end

  it 'load_record should load the specified record (values converted to original data types)' do
    begin
      rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
      helper = ReplicationHelper.new(rep_run)

      rep_run.session.right.insert_record('scanner_records', id: 2,
                                                             name: 'Bob - right database version')

      expect(helper.load_record(:right, 'scanner_records', 'id' => '2')).to eq({
        'id' => '2',
        'name' => 'Bob - right database version'
      })
    ensure
      rep_run.session.right.execute('delete from scanner_records')
    end
  end

  it 'options_for_table should return the correct options for the table' do
    Initializer.configuration.options = { a: 1, b: 2 }
    Initializer.configuration.add_table_options 'scanner_records', b: 3
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    options = helper.options_for_table('scanner_records')
    expect(options[:a]).to eq(1)
    expect(options[:b]).to eq(3)
  end

  it 'options_for_table should merge the configured options into the default two way replicator options' do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    expect(helper.options_for_table('scanner_records').include?(:left_change_handling)).to be_truthy
    expect(helper.options_for_table('scanner_records').include?(:right_change_handling)).to be_truthy
    expect(helper.options_for_table('scanner_records').include?(:replication_conflict_handling)).to be_truthy
  end

  it 'log_replication_outcome should log the replication outcome correctly' do
    session = Session.new
    begin
      rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
      helper = ReplicationHelper.new(rep_run)

      loaders = LoggedChangeLoaders.new(session)

      left_change = LoggedChange.new loaders[:left]
      right_change = LoggedChange.new loaders[:right]
      diff = ReplicationDifference.new loaders
      diff.changes.replace left: left_change, right: right_change
      diff.type = :conflict

      left_change.type = :update
      right_change.type = :delete
      left_change.table = right_change.table = 'extender_combined_key'
      left_change.key = right_change.key = { 'id' => 5 }

      # Verify that the log information are made fitting
      expect(helper).to receive(:fit_description_columns)
            .with('ignore', 'ignored')
            .and_return(%w(ignoreX ignoredY))

      helper.log_replication_outcome diff, 'ignore', 'ignored'

      row = session.left.select_one('select * from rr_logged_events order by id desc')
      expect(row['activity']).to eq('replication')
      expect(row['change_table']).to eq('extender_combined_key')
      expect(row['diff_type']).to eq('conflict')
      expect(row['change_key']).to eq('5')
      expect(row['left_change_type']).to eq('update')
      expect(row['right_change_type']).to eq('delete')
      expect(row['description']).to eq('ignoreX')
      expect(row['long_description']).to eq('ignoredY')
      expect(Time.parse(row['event_time'])).to be <= 10.seconds.ago
    ensure
      session.left.execute('delete from rr_logged_events')
    end
  end

  it 'finalize should be delegated to the committer' do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)

    c = helper.instance_eval { @committer }
    expect(c).to receive(:finalize).with(false)
    helper.finalize(false)
  end
end
