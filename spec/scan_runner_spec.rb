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
    expect(CommandRunner.commands['scan'][:command]).to eq(ScanRunner)
    expect(CommandRunner.commands['scan'][:description]).to be_an_instance_of(String)
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

      expect($stdout.string).to match(/scanner_records.* 5\n/)
      expect($stdout.string).to match(/extender_one_record.* 0\n/)
    ensure
      $stdout = org_stdout
    end
  end

  it "create_processor should create the correct table scanner" do
    scan_runner = ScanRunner.new
    dummy_scan_class = double("scan class")
    expect(dummy_scan_class).to receive(:new).
      with(:dummy_session, "left_table", "right_table").
      and_return(:dummy_table_scanner)
    expect(TableScanHelper).to receive(:scan_class).with(:dummy_session).
      and_return(dummy_scan_class)
    allow(scan_runner).to receive(:session).and_return(:dummy_session)
    expect(scan_runner.create_processor("left_table", "right_table")).
      to eq(:dummy_table_scanner)
  end

  it "summary_description should return a description" do
    expect(ScanRunner.new.summary_description).to be_an_instance_of(String)
  end
end
