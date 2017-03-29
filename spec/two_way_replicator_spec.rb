require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Replicators::TwoWayReplicator do
  before(:each) do
    Initializer.configuration = deep_copy(standard_config)
    Initializer.configuration.options = {:replicator => :two_way}
  end

  it "should register itself" do
    expect(Replicators::replicators[:two_way]).to eq(Replicators::TwoWayReplicator)
  end

  it "initialize should store the replication helper" do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    expect(replicator.rep_helper).to eq(helper)
  end

  it "verify_option should raise descriptive errors" do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    expect {replicator.verify_option(nil, [:valid_value], :my_key, :my_value)}.
      to raise_error(ArgumentError, ':my_value not a valid :my_key option')
    expect {replicator.verify_option(/my_spec/, [:valid_value], :my_key, :my_value)}.
      to raise_error(ArgumentError, '/my_spec/: :my_value not a valid :my_key option')
  end

  it "initialize should throw an error if options are invalid" do
    rep_run = ReplicationRun.new(Session.new, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    base_options = {
      :replicator => :two_way,
      :left_change_handling => :ignore,
      :right_change_handling => :ignore,
      :replication_conflict_handling => :ignore,
      :logged_replication_events => [:ignored_conflicts]
    }

    # Verify that correct options don't raise errors.
    allow(helper).to receive(:options).and_return(base_options)
    expect {Replicators::TwoWayReplicator.new(helper)}.not_to raise_error

    # Also lambda options should not raise errors.
    l = lambda {}
    allow(helper).to receive(:options).and_return(base_options.merge(
        {
          :left_change_handling => l,
          :right_change_handling => l,
          :repliction_conflict_handling => l
        })
    )
    expect {Replicators::TwoWayReplicator.new(helper)}.not_to raise_error

    # Invalid options should raise errors
    invalid_options = [
      {:left_change_handling => :invalid_left_option},
      {:right_change_handling => :invalid_right_option},
      {:replication_conflict_handling => :invalid_conflict_option},
      {:logged_replication_events => :invalid_logging_option},
    ]
    invalid_options.each do |options|
      allow(helper.session.configuration).to receive(:options).and_return(base_options.merge(options))
      expect {Replicators::TwoWayReplicator.new(helper)}.to raise_error(ArgumentError)
    end
  end

  it "log_replication_outcome should log conflicts correctly" do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))

    loaders = LoggedChangeLoaders.new(session)

    diff = ReplicationDifference.new loaders
    diff.type = :conflict
    diff.changes[:left] = LoggedChange.new loaders[:left]
    diff.changes[:left].table = 'scanner_records'

    # should only log events if so configured
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    expect(helper).not_to receive(:log_replication_outcome)
    allow(helper).to receive(:options_for_table).and_return({:logged_replication_events => []})
    replicator.log_replication_outcome :ignore, diff
    allow(helper).to receive(:options_for_table).and_return({:logged_replication_events => [:ignored_conflicts]})
    replicator.log_replication_outcome :left, diff

    # should log ignored conflicts correctly
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    expect(helper).to receive(:log_replication_outcome).with(diff, 'ignored')
    allow(helper).to receive(:options_for_table).and_return({:logged_replication_events => [:ignored_conflicts]})
    replicator.log_replication_outcome :ignore, diff

    # should log conflicts correctly
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    expect(helper).to receive(:log_replication_outcome).with(diff, 'left_won')
    allow(helper).to receive(:options_for_table).and_return({:logged_replication_events => [:all_conflicts]})
    replicator.log_replication_outcome :left, diff
  end

  it "log_replication_outcome should log changes correctly" do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))

    loaders = LoggedChangeLoaders.new(session)

    diff = ReplicationDifference.new loaders
    diff.type = :left
    diff.changes[:left] = LoggedChange.new loaders[:left]
    diff.changes[:left].table = 'scanner_records'

    # should only log events if so configured
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    expect(helper).not_to receive(:log_replication_outcome)
    allow(helper).to receive(:options_for_table).and_return({:logged_replication_events => []})
    replicator.log_replication_outcome :ignore, diff
    allow(helper).to receive(:options_for_table).and_return({:logged_replication_events => [:ignored_changes]})
    replicator.log_replication_outcome :left, diff

    # should log changes correctly
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    expect(helper).to receive(:log_replication_outcome).with(diff, 'replicated')
    allow(helper).to receive(:options_for_table).and_return({:logged_replication_events => [:all_changes]})
    replicator.log_replication_outcome :right, diff

    # should log changes correctly
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    expect(helper).to receive(:log_replication_outcome).with(diff, 'ignored')
    allow(helper).to receive(:options_for_table).and_return({:logged_replication_events => [:ignored_changes]})
    replicator.log_replication_outcome :ignore, diff
  end

  it "replicate_difference should not do anything if ignore option is given" do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    allow(helper).to receive(:options_for_table).and_return(
      {
        :left_change_handling => :ignore,
        :right_change_handling => :ignore,
        :replication_conflict_handling => :ignore,
        :logged_replication_events => [:ignored_changes, :ignored_conflicts]
      }
    )

    loaders = LoggedChangeLoaders.new(session)

    diff = ReplicationDifference.new(loaders)
    diff.changes[:left] = LoggedChange.new loaders[:left]
    diff.changes[:left].table = 'scanner_records'

    # but logging should still happen
    expect(replicator).to receive(:log_replication_outcome).
      with(:ignore, diff).
      exactly(3).times

    expect(helper).not_to receive :insert_record
    expect(helper).not_to receive :update_record
    expect(helper).not_to receive :delete_record

    diff.type = :conflict
    replicator.replicate_difference diff
    diff.type = :left
    replicator.replicate_difference diff
    diff.type = :right
    replicator.replicate_difference diff
  end

  it "replicate_difference should call the provided Proc objects" do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)

    lambda_parameters = []
    l = lambda do |rep_helper, diff|
      lambda_parameters << [rep_helper, diff]
    end
    replicator = Replicators::TwoWayReplicator.new(helper)
    allow(helper).to receive(:options_for_table).and_return(
      {
        :left_change_handling => l,
        :right_change_handling => l,
        :replication_conflict_handling => l
      }
    )

    loaders = LoggedChangeLoaders.new(session)

    change = LoggedChange.new loaders[:left]
    change.table = 'scanner_records'

    d1 = ReplicationDifference.new(loaders)
    d1.type = :conflict
    d1.changes[:left] = change
    replicator.replicate_difference d1

    d2 = ReplicationDifference.new(loaders)
    d2.type = :left
    d2.changes[:left] = change
    replicator.replicate_difference d2

    d3 = ReplicationDifference.new(loaders)
    d3.type = :right
    d3.changes[:left] = change
    replicator.replicate_difference d3

    expect(lambda_parameters).to eq([
      [helper, d1],
      [helper, d2],
      [helper, d3],
    ])
  end

  it "replicate_difference should clear conflicts as per provided options" do
    session = Session.new
    rep_run = ReplicationRun.new(session, TaskSweeper.new(1))
    helper = ReplicationHelper.new(rep_run)

    left_change = LoggedChange.new LoggedChangeLoader.new(session, :left)
    left_change.table = 'scanner_records'
    right_change = LoggedChange.new LoggedChangeLoader.new(session, :right)
    right_change.table = 'scanner_records'
    diff = ReplicationDifference.new(session)
    diff.type = :conflict
    diff.changes[:left] = left_change
    diff.changes[:right] = right_change

    replicator = Replicators::TwoWayReplicator.new(helper)
    allow(helper).to receive(:options_for_table).and_return({:replication_conflict_handling => :left_wins})
    expect(replicator).to receive(:clear_conflict).with(:left, diff, 1)
    replicator.replicate_difference diff, 1

    replicator = Replicators::TwoWayReplicator.new(helper)
    allow(helper).to receive(:options_for_table).and_return({:replication_conflict_handling => :right_wins})
    expect(replicator).to receive(:clear_conflict).with(:right, diff, 1)
    replicator.replicate_difference diff, 1

    replicator = Replicators::TwoWayReplicator.new(helper)
    allow(helper).to receive(:options_for_table).and_return({:replication_conflict_handling => :later_wins})
    expect(replicator).to receive(:clear_conflict).with(:left, diff, 1).twice
    left_change.last_changed_at = 5.seconds.from_now
    right_change.last_changed_at = Time.now
    replicator.replicate_difference diff, 1
    left_change.last_changed_at = right_change.last_changed_at = Time.now
    replicator.replicate_difference diff, 1
    expect(replicator).to receive(:clear_conflict).with(:right, diff, 1)
    right_change.last_changed_at = 5.seconds.from_now
    replicator.replicate_difference diff, 1

    replicator = Replicators::TwoWayReplicator.new(helper)
    allow(helper).to receive(:options_for_table).and_return({:replication_conflict_handling => :earlier_wins})
    expect(replicator).to receive(:clear_conflict).with(:left, diff, 1).twice
    left_change.last_changed_at = 5.seconds.ago
    right_change.last_changed_at = Time.now
    replicator.replicate_difference diff, 1
    left_change.last_changed_at = right_change.last_changed_at = Time.now
    replicator.replicate_difference diff, 1
    expect(replicator).to receive(:clear_conflict).with(:right, diff, 1)
    right_change.last_changed_at = 5.seconds.ago
    replicator.replicate_difference diff, 1
  end

  it "replicate_difference should raise Exception if all replication attempts have been exceeded" do
    rep_run = ReplicationRun.new Session.new, TaskSweeper.new(1)
    helper = ReplicationHelper.new(rep_run)
    replicator = Replicators::TwoWayReplicator.new(helper)
    expect {replicator.replicate_difference :dummy_diff, 0}.
      to raise_error(Exception, "max replication attempts exceeded")
  end
end
