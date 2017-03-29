require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TableScan do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "initialize should cache the primary keys of the given table" do
    session = Session.new
    scann = TableScan.new session, 'scanner_records'
    expect(scann.primary_key_names).to eq(['id'])
  end

  it "initialize should use the name of the left table as overwritable default for right table" do
    session = Session.new
    expect(TableScan.new(session, 'scanner_records').right_table).to eq('scanner_records')
    expect(TableScan.new(session, 'scanner_records', 'dummy').right_table).to eq('dummy')
  end

  it "progress_printer= should store the progress printer class" do
    session = Session.new
    TableScan.new(session, 'scanner_records').progress_printer = :dummy_printer_class
  end
end
