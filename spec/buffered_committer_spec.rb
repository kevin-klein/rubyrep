require File.dirname(__FILE__) + '/spec_helper.rb'
require File.dirname(__FILE__) + "/../config/test_config.rb"

include RR

describe Committers::BufferedCommitter do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "should register itself" do
    expect(Committers.committers[:buffered_commit]).to eq(Committers::BufferedCommitter)
  end

  # Stubs out the starting of transactions in the given Session.
  def stub_begin_transaction(session)
    allow(session.left.transaction_manager).to receive :begin_transaction
    allow(session.right.transaction_manager).to receive :begin_transaction
  end

  # Stubs out the executing of SQL statements for the given Session.
  def stub_execute(session)
    allow(session.left).to receive :execute
    allow(session.right).to receive :execute
  end

  it "trigger_mode_switcher should return and if necessary create the trigger mode switcher" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)
    switcher = committer.trigger_mode_switcher
    expect(switcher).to be_an_instance_of(TriggerModeSwitcher)

    expect(committer.trigger_mode_switcher).to eq(switcher) # ensure it is only created one
  end

  it "exclude_rr_activity should exclude the rubyrep activity for the specified table" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)
    expect(committer.trigger_mode_switcher).to receive(:exclude_rr_activity).with(:left, 'dummy_table')
    committer.exclude_rr_activity :left, 'dummy_table'
  end

  it "activity_marker_table should return the correct table name" do
    config = deep_copy(standard_config)
    config.options[:rep_prefix] = 'rx'
    session = Session.new config
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)
    expect(committer.activity_marker_table).to eq('rx_running_flags')
  end

  it "maintain_activity_status should return true if activity marker table exists" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)
    expect(committer.maintain_activity_status?).to be_truthy
  end

  it "maintain_activity_status should return false if activity marker does not exist" do
    config = deep_copy(standard_config)
    config.options[:rep_prefix] = 'rxfdsfkdsf'
    session = Session.new config
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)
    expect(committer.maintain_activity_status?).to be_falsey
  end

  it "commit_frequency should return the configured commit frequency" do
    config = deep_copy(standard_config)
    config.options[:commit_frequency] = 5
    session = Session.new config
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)
    expect(committer.commit_frequency).to eq(5)
  end

  it "commit_frequency should return the the default commit frequency if nothing else is configured" do
    config = deep_copy(standard_config)
    config.options.delete :commit_frequency
    session = Session.new config
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)
    expect(committer.commit_frequency).to eq(Committers::BufferedCommitter::DEFAULT_COMMIT_FREQUENCY)
  end

  it "commit_db_transactions should commit the transactions in both databases" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)

    expect(session.left.transaction_manager).to receive(:commit_transaction)
    expect(session.right.transaction_manager).to receive(:commit_transaction)
    committer.commit_db_transactions
  end

  it "commit_db_transactions should clear the activity marker table" do
    session = Session.new
    stub_begin_transaction session
    allow(session.left.transaction_manager).to receive(:commit_transaction)
    allow(session.right.transaction_manager).to receive(:commit_transaction)
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)

    expect(session.left).to receive(:execute).with("delete from rr_running_flags")
    expect(session.right).to receive(:execute).with("delete from rr_running_flags")
    committer.commit_db_transactions
  end

  it "commit_db_transactions should not clear the activity marker table if it doesn't exist" do
    config = deep_copy(standard_config)
    config.options[:rep_prefix] = 'fsdtrie9g'
    session = Session.new config
    stub_begin_transaction session
    allow(session.left.transaction_manager).to receive(:commit_transaction)
    allow(session.right.transaction_manager).to receive(:commit_transaction)
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)

    expect(session.left).not_to receive(:execute)
    expect(session.right).not_to receive(:execute)
    committer.commit_db_transactions
  end

  it "begin_db_transactions should begin new transactions in both databases" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)

    expect(session.left.transaction_manager).to receive(:begin_transaction)
    expect(session.right.transaction_manager).to receive(:begin_transaction)
    committer.begin_db_transactions
  end

  it "begin_db_transactions should insert a record into the activity marker table" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)

    expect(session.left).to receive(:execute).with("insert into rr_running_flags values(1)")
    expect(session.right).to receive(:execute).with("insert into rr_running_flags values(1)")
    committer.begin_db_transactions
  end

  it "begin_db_transactions should not clear the activity marker table if it doesn't exist" do
    config = deep_copy(standard_config)
    config.options[:rep_prefix] = 'triutiuerioge'
    session = Session.new config
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)

    expect(session.left).not_to receive(:execute)
    expect(session.right).not_to receive(:execute)
    committer.begin_db_transactions
  end

  it "rollback_db_transactions should roll back the transactions in both databases" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)

    expect(session.left.transaction_manager).to receive(:rollback_transaction)
    expect(session.right.transaction_manager).to receive(:rollback_transaction)
    committer.rollback_db_transactions
  end

  it "commit should only commit and start new transactions if the specified number of changes have been executed" do
    config = deep_copy(standard_config)
    config.options[:commit_frequency] = 2
    session = Session.new config
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)

    expect(committer).to receive(:commit_db_transactions).twice
    expect(committer).to receive(:begin_db_transactions).twice
    committer.commit
    expect(committer.new_transaction?).to be_falsey
    3.times {committer.commit}
    expect(committer.new_transaction?).to be_truthy
  end

  it "insert_record should commit" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)

    expect(committer).to receive(:exclude_rr_activity).with(:right, 'right_table').ordered
    expect(session.right).to receive(:insert_record).with('right_table', :dummy_values).ordered
    expect(committer).to receive(:commit).ordered

    committer.insert_record(:right, 'right_table', :dummy_values)
  end

  it "update_record should commit" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)

    expect(committer).to receive(:exclude_rr_activity).with(:right, 'right_table').ordered
    expect(session.right).to receive(:update_record).with('right_table', :dummy_values, :dummy_org_key).ordered
    expect(committer).to receive(:commit).ordered

    committer.update_record(:right, 'right_table', :dummy_values, :dummy_org_key)
  end

  it "delete_record should commit" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)

    expect(committer).to receive(:exclude_rr_activity).with(:right, 'right_table').ordered
    expect(session.right).to receive(:delete_record).with('right_table', :dummy_values).ordered
    expect(committer).to receive(:commit).ordered

    committer.delete_record(:right, 'right_table', :dummy_values)
  end

  it "finalize should commit the transactions if called with success = true" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)

    expect(committer).to receive(:commit_db_transactions)

    committer.finalize true
  end

  it "finalize should rollbackup the transactions if called with success = false" do
    session = Session.new
    stub_begin_transaction session
    stub_execute session
    committer = Committers::BufferedCommitter.new(session)

    expect(committer).to receive(:rollback_db_transactions)

    committer.finalize false
  end
end
