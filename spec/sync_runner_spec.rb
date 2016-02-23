require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe SyncRunner do
  before(:each) do
    session = Session.new

    session.left.execute('delete from scanner_records')
    session.right.execute('delete from scanner_records')

    session.left.insert_record('scanner_records', {
      id: 2,
      name: 'Bob - left database version'
    })

    session.left.insert_record('scanner_records', {
      id: 3,
      name: 'Charlie - exists in left database only'
    })

    session.left.insert_record('scanner_records', {
      id: 5,
      name: 'Eve - exists in left database only'
    })

    session.right.insert_record('scanner_records', {
      id: 2,
      name: 'Bob - right database version'
    })

    session.right.insert_record('scanner_records', {
      id: 4,
      name: 'Dave - exists in right database only'
    })

    session.right.insert_record('scanner_records', {
      id: 6,
      name: 'Fred - exists in right database only'
    })
  end

  after(:each) do
    session = Session.new

    session.left.execute('delete from scanner_records')
    session.right.execute('delete from scanner_records')
  end

  it "should register itself with CommandRunner" do
    CommandRunner.commands['sync'][:command].should == SyncRunner
    CommandRunner.commands['sync'][:description].should be_an_instance_of(String)
  end

  it "prepare_table_pairs should sort the tables" do
    session = Session.new standard_config
    session.should_receive(:sort_table_pairs).
      with(:dummy_table_pairs).
      and_return(:sorted_dummy_table_pairs)

    sync_runner = SyncRunner.new
    sync_runner.stub!(:session).and_return(session)

    sync_runner.prepare_table_pairs(:dummy_table_pairs).should == :sorted_dummy_table_pairs
  end

  it "execute should sync the specified tables" do
    org_stdout = $stdout
    session = nil

    # This is necessary to avoid the cached RubyRep configurations from getting
    # overwritten by the sync run
    old_config, Initializer.configuration = Initializer.configuration, Configuration.new

    session = Session.new(standard_config)

    $stdout = StringIO.new
    begin
      sync_runner = SyncRunner.new
      sync_runner.options = {
        :config_file => "#{File.dirname(__FILE__)}/../config/test_config.rb",
        :table_specs => ["scanner_records"]
      }

      sync_runner.execute

      $stdout.string.should =~
        /scanner_records .* 5\n/

      left_records = session.left.connection.select_all("select * from scanner_records order by id").to_hash
      right_records = session.right.connection.select_all("select * from scanner_records order by id").to_hash
      left_records.should == right_records
    ensure
      $stdout = org_stdout
      Initializer.configuration = old_config if old_config
    end
  end

  it "create_processor should create the TableSync instance" do
    TableSync.should_receive(:new).
      with(:dummy_session, "left_table", "right_table").
      and_return(:dummy_table_sync)
    sync_runner = SyncRunner.new
    sync_runner.should_receive(:session).and_return(:dummy_session)
    sync_runner.create_processor("left_table", "right_table").
      should == :dummy_table_sync
  end

  it "summary_description should return a description" do
    SyncRunner.new.summary_description.should be_an_instance_of(String)
  end

end
