require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ScanRunner do
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

    session.left.execute('delete from extender_one_record')
    session.right.execute('delete from extender_one_record')
  end

  after(:each) do
    session = Session.new

    session.left.execute('delete from scanner_records')
    session.right.execute('delete from scanner_records')
  end

  it "should register itself with CommandRunner" do
    CommandRunner.commands['scan'][:command].should == ScanRunner
    CommandRunner.commands['scan'][:description].should be_an_instance_of(String)
  end

  it "execute should scan the specified tables" do
    org_stdout = $stdout
    $stdout = StringIO.new
    begin
      Initializer.configuration = Configuration.new
      scan_runner = ScanRunner.new
      scan_runner.options = {
        :config_file => "#{File.dirname(__FILE__)}/../config/test_config.rb",
        :table_specs => ["scanner_records", "extender_one_record"]
      }

      scan_runner.execute

      $stdout.string.should =~ /scanner_records.* 5\n/
      $stdout.string.should =~ /extender_one_record.* 0\n/
    ensure
      $stdout = org_stdout
    end
  end

  it "create_processor should create the correct table scanner" do
    scan_runner = ScanRunner.new
    dummy_scan_class = mock("scan class")
    dummy_scan_class.should_receive(:new).
      with(:dummy_session, "left_table", "right_table").
      and_return(:dummy_table_scanner)
    TableScanHelper.should_receive(:scan_class).with(:dummy_session).
      and_return(dummy_scan_class)
    scan_runner.should_receive(:session).any_number_of_times.and_return(:dummy_session)
    scan_runner.create_processor("left_table", "right_table").
      should == :dummy_table_scanner
  end

  it "summary_description should return a description" do
    ScanRunner.new.summary_description.should be_an_instance_of(String)
  end
end
