require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe DirectTableScan do
  before(:each) do
    Initializer.configuration = standard_config

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

  it "run should compare all the records in the table" do
    session = Session.new

    scan = DirectTableScan.new session, 'scanner_records'
    diff = []
    scan.run do |type, row|
      diff.push [type, row]
    end
    # in this scenario the right table has the 'highest' data,
    # so 'right-sided' data are already implicitely tested here
    expect(diff).to eq([
      [:conflict, [
          {'id' => '2', 'name' => 'Bob - left database version'},
          {'id' => '2', 'name' => 'Bob - right database version'}]],
      [:left, {'id' => '3', 'name' => 'Charlie - exists in left database only'}],
      [:right, {'id' => '4', 'name' => 'Dave - exists in right database only'}],
      [:left, {'id' => '5', 'name' => 'Eve - exists in left database only'}],
      [:right, {'id' => '6', 'name' => 'Fred - exists in right database only'}]
    ])
  end

  it "run should handle one-sided data" do
    # separate test case for left-sided data; right-sided data are already covered in the general test
    begin
      session = Session.new
      session.left.execute('delete from scanner_left_records_only')
      session.right.execute('delete from scanner_left_records_only')
      session.left.insert_record('scanner_left_records_only', {
          id: 1,
          name: 'Alice'
      })
      session.left.insert_record('scanner_left_records_only', {
          id: 2,
          name: 'Bob'
      })

      scan = DirectTableScan.new session, 'scanner_left_records_only'
      diff = []
      scan.run do |type, row|
        diff.push [type, row]
      end
      expect(diff).to eq([
        [:left, {'id' => '1', 'name' => 'Alice'}],
        [:left, {'id' => '2', 'name' => 'Bob'}]
      ])
    ensure
      session.left.execute('delete from scanner_left_records_only')
      session.right.execute('delete from scanner_left_records_only')
    end
  end

  it "run should update the progress" do
    session = Session.new
    scan = DirectTableScan.new session, 'scanner_records'
    number_steps = 0
    allow(scan).to receive(:update_progress) do |steps|
      number_steps += steps
    end
    scan.run {|_, _|}
    expect(number_steps).to eq(6)
  end

  it "run should update the progress even if there are no records" do
    # it should do that to ensure the progress bar is printed
    scan = DirectTableScan.new Session.new, 'extender_no_record'
    expect(scan).to receive(:update_progress).at_least(:once)
    scan.run {|_, _|}
  end
end
