require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ReplicationInitializer do
  before(:each) do
    Initializer.configuration = standard_config
  end

  after(:each) do
  end

  it 'initializer should store the session' do
    session = Session.new
    initializer = ReplicationInitializer.new session
    expect(initializer.session).to eq(session)
  end

  it 'options should return the table specific options if table is given' do
    session = Session.new deep_copy(Initializer.configuration)
    initializer = ReplicationInitializer.new session
    expect(session.configuration).to receive(:options_for_table)
           .with('my_table')
           .and_return(:dummy_options)
    expect(initializer.options('my_table')).to eq(:dummy_options)
  end

  it 'options should return the general options if no table is given' do
    session = Session.new deep_copy(Initializer.configuration)
    initializer = ReplicationInitializer.new session
    expect(session.configuration).to receive(:options)
           .and_return(:dummy_options)
    expect(initializer.options).to eq(:dummy_options)
  end

  it 'create_trigger should create a working trigger' do
    session = nil
    begin
      session = Session.new
      initializer = ReplicationInitializer.new(session)

      if session.left.replication_trigger_exists?('rr_trigger_test', 'trigger_test')
        session.left.drop_replication_trigger('rr_trigger_test', 'trigger_test')
      end
      session.left.execute('delete from rr_pending_changes')

      initializer.create_trigger(:left, 'trigger_test')

      session.left.insert_record('trigger_test', 'first_id' => 1,
                                                 'second_id' => 2,
                                                 'name' => 'bla',
                                                 'id' => 2)

      row = session.left.select_one('select * from rr_pending_changes')
      row.delete 'id'
      row.delete 'change_time'
      expect(row).to eq({
        'change_table' => 'trigger_test',
        'change_key' => 'id|2',
        'change_new_key' => nil,
        'change_type' => 'I'
      })
    ensure
      session.left.execute 'delete from trigger_test'
      session.left.execute 'delete from rr_pending_changes'
    end
  end

  it 'trigger_exists? and drop_trigger should work correctly' do
    session = nil
    begin
      session = Session.new
      initializer = ReplicationInitializer.new(session)
      if initializer.trigger_exists?(:left, 'trigger_test')
        initializer.drop_trigger(:left, 'trigger_test')
      end

      initializer.create_trigger :left, 'trigger_test'
      expect(initializer.trigger_exists?(:left, 'trigger_test'))
                 .to be_truthy
      initializer.drop_trigger(:left, 'trigger_test')
      expect(initializer.trigger_exists?(:left, 'trigger_test'))
                 .to be_falsey
    ensure
    end
  end

  it 'ensure_sequence_setup should not do anything if :adjust_sequences option is not given' do
    config = deep_copy(Initializer.configuration)
    config.add_table_options 'sequence_test', adjust_sequences: false
    session = Session.new(config)
    initializer = ReplicationInitializer.new(session)

    expect(session.left).not_to receive(:update_sequences)
    expect(session.right).not_to receive(:update_sequences)

    table_pair = { left: 'sequence_test', right: 'sequence_test' }
    initializer.ensure_sequence_setup table_pair, 3, 2, 2
  end

  it "ensure_sequence_setup should ensure that a table's auto generated ID values have the correct increment and offset" do
    session = nil
    begin
      session = Session.new
      initializer = ReplicationInitializer.new(session)

      session.left.execute 'delete from sequence_test'
      session.right.execute 'delete from sequence_test'

      # Note:
      # Calling ensure_sequence_setup twice with different values to ensure that
      # it is actually does something.

      table_pair = { left: 'sequence_test', right: 'sequence_test' }

      initializer.ensure_sequence_setup table_pair, 3, 2, 2
      initializer.ensure_sequence_setup table_pair, 5, 2, 1
      id1, id2 = get_example_sequence_values(session)
      expect(id2 - id1).to eq(5)
      expect(id1 % 5).to eq(2)
    ensure
      [:left, :right].each do |database|
        initializer.clear_sequence_setup database, 'sequence_test'
        session.send(database).execute 'delete from sequence_test'
      end
    end
  end

  it 'clear_sequence_setup should not do anything if :adjust_sequences option is not given' do
    config = deep_copy(Initializer.configuration)
    config.add_table_options 'sequence_test', adjust_sequences: false
    session = Session.new(config)
    initializer = ReplicationInitializer.new(session)

    expect(session.left).not_to receive(:clear_sequence_setup)

    initializer.clear_sequence_setup :left, 'sequence_test'
  end

  it 'clear_sequence_setup should remove custom sequence settings' do
    session = nil
    begin
      session = Session.new
      initializer = ReplicationInitializer.new(session)
      table_pair = { left: 'sequence_test', right: 'sequence_test' }
      initializer.ensure_sequence_setup table_pair, 5, 2, 2
      initializer.clear_sequence_setup :left, 'sequence_test'
      id1, id2 = get_example_sequence_values(session)
      expect(id2 - id1).to eq(1)
    ensure
      [:left, :right].each do |database|
        initializer.clear_sequence_setup database, 'sequence_test'
        session.send(database).execute 'delete from sequence_test'
      end
    end
  end

  it 'change_log_exists? should return true if replication log exists' do
    config = deep_copy(standard_config)
    initializer = ReplicationInitializer.new(Session.new(config))
    expect(initializer.change_log_exists?(:left)).to be_truthy
    config.options[:rep_prefix] = 'r2'
    initializer = ReplicationInitializer.new(Session.new(config))
    expect(initializer.change_log_exists?(:left)).to be_falsey
  end

  it 'event_log_exists? should return true if event log exists' do
    config = deep_copy(standard_config)
    initializer = ReplicationInitializer.new(Session.new(config))
    expect(initializer.event_log_exists?).to be_truthy
    config.options[:rep_prefix] = 'r2'
    initializer = ReplicationInitializer.new(Session.new(config))
    expect(initializer.event_log_exists?).to be_falsey
  end

  it 'create_event_log / drop_event_log should create / drop the event log' do
    config = deep_copy(standard_config)
    config.options[:rep_prefix] = 'r2'
    session = Session.new(config)
    initializer = ReplicationInitializer.new(session)
    initializer.drop_logged_events if initializer.event_log_exists?

    allow($stderr).to receive :write
    expect(initializer.event_log_exists?).to be_falsey
    initializer.create_event_log
    expect(initializer.event_log_exists?).to be_truthy

    # verify that replication log has 8 byte, auto-generating primary key
    session.left.insert_record 'r2_logged_events', 'id' => 1e18.to_i, 'change_key' => 'blub'
    expect(session.left.select_one("select id from r2_logged_events where change_key = 'blub'")['id']
           .to_i).to eq(1e18.to_i)

    initializer.drop_event_log
    expect(initializer.event_log_exists?).to be_falsey
  end

  it 'create_change_log / drop_change_log should create / drop the replication log' do
    config = deep_copy(standard_config)
    config.options[:rep_prefix] = 'r2'
    session = Session.new(config)
    initializer = ReplicationInitializer.new(session)
    initializer.drop_change_log(:left) if initializer.change_log_exists?(:left)

    allow($stderr).to receive :write
    expect(initializer.change_log_exists?(:left)).to be_falsey
    initializer.create_change_log(:left)
    expect(initializer.change_log_exists?(:left)).to be_truthy

    # verify that replication log has 8 byte, auto-generating primary key
    session.left.insert_record 'r2_pending_changes', 'change_key' => 'bla'
    expect(session.left.select_one("select id from r2_pending_changes where change_key = 'bla'")['id']
           .to_i).to be > 0
    session.left.insert_record 'r2_pending_changes', 'id' => 1e18.to_i, 'change_key' => 'blub'
    expect(session.left.select_one("select id from r2_pending_changes where change_key = 'blub'")['id']
           .to_i).to eq(1e18.to_i)

    initializer.drop_change_log(:left)
    expect(initializer.change_log_exists?(:left)).to be_falsey
  end

  it 'ensure_activity_markers should not create the tables if they already exist' do
    session = Session.new
    initializer = ReplicationInitializer.new(session)
    expect(session.left).not_to receive(:create_table)
    initializer.ensure_activity_markers
  end

  it 'ensure_activity_markers should create the marker tables' do
    begin
      config = deep_copy(standard_config)
      config.options[:rep_prefix] = 'rx'
      session = Session.new(config)
      initializer = ReplicationInitializer.new(session)
      initializer.ensure_activity_markers
      expect(session.left.tables.include?('rx_running_flags')).to be_truthy
      expect(session.right.tables.include?('rx_running_flags')).to be_truthy

      # right columns?
      columns = session.left.columns('rx_running_flags')
      expect(columns.size).to eq(1)
      expect(columns[0].name).to eq('active')
    ensure
      if session
        session.left.drop_table 'rx_running_flags'
        session.right.drop_table 'rx_running_flags'
      end
    end
  end

  it 'ensure_infrastructure should not create the infrastructure tables if they already exist' do
    session = Session.new
    initializer = ReplicationInitializer.new(session)
    expect(session.left).not_to receive(:create_table)
    initializer.ensure_infrastructure
  end

  it 'drop_change_logs should drop the change_log tables' do
    session = Session.new
    initializer = ReplicationInitializer.new session
    expect(initializer).to receive(:drop_change_log).with(:left)
    expect(initializer).to receive(:drop_change_log).with(:right)

    initializer.drop_change_logs
  end

  it 'drop_change_logs should not do anything if change_log tables do not exist' do
    config = deep_copy(standard_config)
    config.options[:rep_prefix] = 'rx'
    session = Session.new(config)
    initializer = ReplicationInitializer.new session
    expect(initializer).not_to receive(:drop_change_log).with(:left)
    expect(initializer).not_to receive(:drop_change_log).with(:right)

    initializer.drop_change_logs
  end

  it 'drop_activity_markers should drop the activity_marker tables' do
    session = Session.new
    initializer = ReplicationInitializer.new session
    expect(session.left).to receive(:drop_table).with('rr_running_flags')
    expect(session.right).to receive(:drop_table).with('rr_running_flags')

    initializer.drop_activity_markers
  end

  it 'drop_activity_markers should not do anything if the activity_marker tables do not exist' do
    config = deep_copy(standard_config)
    config.options[:rep_prefix] = 'rx'
    session = Session.new(config)
    initializer = ReplicationInitializer.new session
    expect(session.left).not_to receive(:drop_table).with('rr_running_flags')
    expect(session.right).not_to receive(:drop_table).with('rr_running_flags')

    initializer.drop_change_logs
  end

  it 'drop_infrastructure should drop all infrastructure tables' do
    session = Session.new
    initializer = ReplicationInitializer.new session
    expect(initializer).to receive(:drop_event_log)
    expect(initializer).to receive(:drop_change_logs)
    expect(initializer).to receive(:drop_activity_markers)

    initializer.drop_infrastructure
  end

  it 'ensure_change_logs should create the change_log tables' do
    session = nil
    begin
      config = deep_copy(standard_config)
      config.options[:rep_prefix] = 'rx'
      session = Session.new(config)
      initializer = ReplicationInitializer.new(session)
      initializer.ensure_change_logs
    ensure
      if session
        session.left.drop_table 'rx_pending_changes'
        session.right.drop_table 'rx_pending_changes'
      end
    end
  end

  it 'ensure_change_logs should do nothing if the change_log tables already exist' do
    session = Session.new
    initializer = ReplicationInitializer.new session
    expect(initializer).not_to receive(:create_change_log)

    initializer.ensure_change_logs
  end

  it 'ensure_event_log should create the event_log table' do
    session = nil
    begin
      config = deep_copy(standard_config)
      config.options[:rep_prefix] = 'rx'
      session = Session.new(config)
      initializer = ReplicationInitializer.new(session)
      initializer.ensure_event_log
    ensure
      session.left.drop_table 'rx_logged_events' if session
    end
  end

  it 'ensure_event_log should do nothing if the event_log table already exist' do
    session = Session.new
    initializer = ReplicationInitializer.new session
    expect(initializer).not_to receive(:create_event_log)

    initializer.ensure_event_log
  end

  it 'ensure_infrastructure should create the infrastructure tables' do
    session = Session.new
    initializer = ReplicationInitializer.new(session)
    expect(initializer).to receive :ensure_activity_markers
    expect(initializer).to receive :ensure_change_logs
    expect(initializer).to receive :ensure_event_log
    initializer.ensure_infrastructure
  end

  it 'call_after_init_handler should call the according handler' do
    config = deep_copy(standard_config)
    received_session = nil
    config.options[:after_infrastructure_setup] = lambda do |session|
      received_session = session
    end
    session = Session.new config
    initializer = ReplicationInitializer.new session
    initializer.call_after_infrastructure_setup_handler

    expect(received_session).to eq(session)
  end

  it 'exclude_ruby_rep_tables should exclude the correct system tables' do
    config = deep_copy(standard_config)
    initializer = ReplicationInitializer.new(Session.new(config))
    initializer.exclude_rubyrep_tables
    expect(initializer.session.configuration.excluded_table_specs.include?(/^rr_.*/)).to be_truthy
  end

  it 'prepare_replication should prepare the replication' do
    session = nil
    initializer = nil
    org_stdout = $stdout

    config = deep_copy(standard_config)
    config.options[:committer] = :buffered_commit
    config.options[:use_ansi] = false

    received_session = nil
    config.options[:after_infrastructure_setup] = lambda do |session|
      received_session = session
    end

    config.include_tables 'rr_pending_changes' # added to verify that it is ignored
    config.exclude_tables('table_with_strange_key')
    config.exclude_tables('referenced_table')
    config.exclude_tables('extender_inverted_combined_key')
    config.exclude_tables('extender_without_key')
    config.exclude_tables('scanner_text_key')

    # added to verify that a disabled :initial_sync is honored
    config.add_table_options 'table_with_manual_key', initial_sync: false

    session = Session.new(config)
    session.left.execute('delete from rr_pending_changes')
    session.right.execute('delete from rr_pending_changes')

    # dummy data to verify that 'table_with_manual_key' is indeed not synced
    session.left.insert_record 'table_with_manual_key', id: 1, name: 'bla'

    $stdout = StringIO.new
    begin
      initializer = ReplicationInitializer.new(session)
      allow(initializer).to receive(:ensure_infrastructure)
      allow(initializer).to receive(:restore_unconfigured_tables)
      initializer.prepare_replication

      expect(received_session).to eq(session)

      # verify sequences have been setup
      expect(session.left.sequence_values('rr', 'scanner_left_records_only').values[0][:increment]).to eq(2)
      expect(session.right.sequence_values('rr', 'scanner_left_records_only').values[0][:increment]).to eq(2)

      # verify table was synced
      left_records = session.left.select_all('select * from  scanner_left_records_only order by id').to_hash
      right_records = session.left.select_all('select * from  scanner_left_records_only order by id').to_hash
      expect(left_records).to eq(right_records)

      # verify rubyrep activity is _not_ logged
      expect(session.right.select_all('select * from rr_pending_changes')).to be_empty

      # verify other data changes are logged
      expect(initializer.trigger_exists?(:left, 'scanner_left_records_only')).to be_truthy
      session.left.insert_record 'scanner_left_records_only', 'id' => 10, 'name' => 'bla'
      changes = session.left.select_all('select change_key from rr_pending_changes')
      expect(changes.count).to eq(1)
      expect(changes[0]['change_key']).to eq('id|10')

      # verify that the 'rr_pending_changes' table was not touched
      expect(initializer.trigger_exists?(:left, 'rr_pending_changes')).to be_falsey

      # verify that initial_sync: false is honored
      expect(session.right.select_all('select * from table_with_manual_key')).to be_empty

      # verify that syncing is done only for unsynced tables
      expect(SyncRunner).not_to receive(:new)
      initializer.prepare_replication

    ensure
      $stdout = org_stdout
      if session
        session.left.execute 'delete from table_with_manual_key'
        session.left.execute 'delete from scanner_left_records_only where id = 10'
        session.right.execute 'delete from scanner_left_records_only'
        [:left, :right].each do |database|
          session.send(database).execute 'delete from rr_pending_changes'
        end
      end
      if initializer
        [:left, :right].each do |database|
          initializer.clear_sequence_setup database, 'scanner_left_records_only'
          initializer.clear_sequence_setup database, 'table_with_manual_key'
          %w(scanner_left_records_only table_with_manual_key).each do |table|
            if initializer.trigger_exists?(database, table)
              initializer.drop_trigger database, table
            end
          end
        end
      end
    end
  end
end
