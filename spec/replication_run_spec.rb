require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ReplicationRun do
  before(:each) do
    Initializer.configuration = standard_config
    session = Session.new
    session.left.execute "delete from rr_pending_changes"
    session.right.execute "delete from rr_logged_events"
    session.left.execute('delete from extender_no_record')
    session.right.execute('delete from extender_no_record')
  end

  let(:session) { Session.new }
  let(:sweeper) { TaskSweeper.new(1) }

  it "initialize should store the provided session" do
    run = ReplicationRun.new(session, sweeper)

    expect(run.session).to eq(session)
  end

  it "install_sweeper should install a task sweeper into the database connections" do
    run = ReplicationRun.new session, sweeper

    expect(session.left.sweeper).to eq(sweeper)
    expect(session.right.sweeper).to eq(sweeper)
  end

  it "helper should return the correctly initialized replication helper" do
    run = ReplicationRun.new(session, sweeper)

    expect(run.helper).to be_an_instance_of(ReplicationHelper)
    expect(run.helper.replication_run).to eq(run)
    expect(run.helper).to eq(run.helper) # ensure the helper is created only once
  end

  it "replicator should return the configured replicator" do
    run = ReplicationRun.new(session, sweeper)

    expect(run.replicator).to be_an_instance_of(Replicators.replicators[session.configuration.options[:replicator]])

    expect(run.replicator).to eq(run.replicator) # should only create the replicator once
    expect(run.replicator.rep_helper).to eq(run.helper)
  end

  it "event_filtered? should behave correctly" do
    begin
      config = deep_copy(standard_config)
      session = Session.new(config)

      session.left.execute('delete from extender_no_record')

      session.left.insert_record 'extender_no_record', {
        'id' => '1',
        'name' => 'bla'
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }

      loaders = LoggedChangeLoaders.new(session)
      loaders.update
      diff = ReplicationDifference.new loaders
      diff.load

      # No event filter at all
      run = ReplicationRun.new session, TaskSweeper.new(1)
      run.event_filtered?(diff).should be_falsey

      # Event filter that does not handle replication events
      session.configuration.options[:event_filter] = Object.new
      run = ReplicationRun.new session, TaskSweeper.new(1)
      run.event_filtered?(diff).should be_falsey

      # event_filtered? should signal filtering (i. e. return true) if filter returns false.
      filter = Object.new
      def filter.before_replicate(table, key, helper, diff)
        false
      end
      session.configuration.options[:event_filter] = filter
      run = ReplicationRun.new session, TaskSweeper.new(1)
      run.event_filtered?(diff).should be_truthy

      # event_filtered? should return false if filter returns true.
      filter = {}
      def filter.before_replicate(table, key, helper, diff)
        self[:args] = [table, key, helper, diff]
        true
      end
      session.configuration.options[:event_filter] = filter
      run = ReplicationRun.new session, TaskSweeper.new(1)
      run.event_filtered?(diff).should be_falsey

      expect(filter[:args][0]).to eq('extender_no_record')
    ensure
      session.left.execute "delete from extender_no_record"
      session.right.execute "delete from extender_no_record"
      session.left.execute "delete from rr_pending_changes"
      session.left.execute('delete from referencing_table')
    end
  end

  it "run should not replicate filtered changes" do
    begin
      config = deep_copy(standard_config)
      config.options[:committer] = :never_commit

      filter = Object.new
      def filter.before_replicate(table, key, helper, diff)
        diff.changes[:left].key['id'] != '1'
      end
      config.options[:event_filter] = filter

      session = Session.new(config)

      session.left.insert_record 'extender_no_record', {
        'id' => '1',
        'name' => 'bla'
      }
      session.left.insert_record 'extender_no_record', {
        'id' => '2',
        'name' => 'blub'
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'I',
        'change_time' => Time.now
      }
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|2',
        'change_type' => 'I',
        'change_time' => Time.now
      }

      run = ReplicationRun.new session, TaskSweeper.new(1)
      run.run

      session.right.select_records(:table => "extender_no_record").should == [{
        'id' => '2',
        'name' => 'blub'
      }]
    ensure
      if session
        session.left.execute "delete from extender_no_record"
        session.right.execute "delete from extender_no_record"
        session.left.execute "delete from rr_pending_changes"
      end
    end
  end

  it "run should not create the replicator if there are no pending changes" do
    session = Session.new
    run = ReplicationRun.new session, TaskSweeper.new(1)
    expect(run).not_to receive(:replicator)
    run.run
  end

  it "run should only replicate real differences" do
    session = Session.new
    begin

      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      session.right.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }

      run = ReplicationRun.new session, TaskSweeper.new(1)
      expect(run.replicator).not_to receive(:replicate)
      run.run

    ensure
      session.left.execute('delete from rr_pending_changes')
      session.right.execute('delete from rr_pending_changes')
    end
  end

  it "run should log raised exceptions" do
    session = Session.new
    begin
      session.left.execute "delete from rr_pending_changes"
      session.left.execute "delete from rr_logged_events"
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      run = ReplicationRun.new(session, sweeper)
      allow(run.replicator).to receive(:replicate_difference) { raise Exception, 'dummy message' }
      run.run

      row = session.left.select_one("select * from rr_logged_events")

      expect(row['description']).to eq('dummy message')
      expect(row['long_description']).to include('Exception')
    ensure
      session.left.execute "delete from rr_pending_changes"
      session.left.execute "delete from rr_logged_events"
      session.right.execute "delete from rr_pending_changes"
      session.right.execute "delete from rr_logged_events"
    end
  end

  it "run should re-raise original exception if logging to database fails" do
    begin
      session.left.execute "delete from rr_pending_changes"
      session.left.execute "delete from rr_logged_events"
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      run = ReplicationRun.new session, TaskSweeper.new(1)

      allow(run.replicator).to receive(:replicate_difference) { raise Exception, 'dummy message' }
      allow(run.helper).to receive(:log_replication_outcome) { raise Exception, 'blub' }

      lambda {run.run}.should raise_error(Exception, 'dummy message')
    ensure
      session.left.execute "delete from rr_pending_changes"
      session.left.execute "delete from rr_logged_events"
      session.right.execute "delete from rr_pending_changes"
      session.right.execute "delete from rr_logged_events"
    end
  end

  it "run should return silently if timed out before work actually started" do
    session = Session.new
    begin
      session.left.execute "delete from rr_pending_changes"
      session.left.insert_record 'rr_pending_changes', {
        'change_table' => 'extender_no_record',
        'change_key' => 'id|1',
        'change_type' => 'D',
        'change_time' => Time.now
      }
      sweeper = TaskSweeper.new(1)
      allow(sweeper).to receive(:terminated?).and_return(true)
      run = ReplicationRun.new session, sweeper
      LoggedChangeLoaders.should_not_receive(:new)
      run.run
    ensure
      session.left.execute "delete from rr_pending_changes"
      session.right.execute "delete from rr_pending_changes"
    end
  end

  it "run should not catch exceptions raised during replicator initialization" do
    config = deep_copy(standard_config)
    config.options[:logged_replication_events] = [:invalid_option]
    session = Session.new config

    session.left.insert_record 'rr_pending_changes', {
      'change_table' => 'extender_no_record',
      'change_key' => 'id|1',
      'change_type' => 'D',
      'change_time' => Time.now
    }

    run = ReplicationRun.new session, TaskSweeper.new(1)
    lambda {run.run}.should raise_error(ArgumentError)
  end

  it "run should process trigger created change log records" do
    begin
      config = deep_copy(standard_config)
      config.options[:committer] = :never_commit
      config.options[:logged_replication_events] = [:all_changes]

      session = Session.new(config)
      session.left.execute "delete from rr_logged_events"
      initializer = ReplicationInitializer.new(session)
      initializer.create_trigger :left, 'extender_no_record'

      session.left.insert_record 'extender_no_record', {
        'id' => '1',
        'name' => 'bla'
      }

      run = ReplicationRun.new session, TaskSweeper.new(1)
      run.run

      session.right.select_record(:table => "extender_no_record").should == {
        'id' => '1',
        'name' => 'bla'
      }

      # also verify that event was logged
      row = session.left.select_one("select * from rr_logged_events")
      row['diff_type'].should == 'left'
      row['change_key'].should == '1'
      row['description'].should == 'replicated'
    ensure
      initializer.drop_trigger :left, 'extender_no_record' if initializer
    end
  end
end
