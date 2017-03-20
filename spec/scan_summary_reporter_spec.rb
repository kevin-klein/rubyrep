require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ScanReportPrinters::ScanSummaryReporter do
  before(:each) do
    allow($stdout).to receive(:puts)
  end

  it "should register itself with ScanRunner" do
    expect(RR::ScanReportPrinters.printers.any? do |printer|
      printer[:printer_class] == ScanReportPrinters::ScanSummaryReporter
    end).to be_truthy
  end
  
  it "initialize should detect if the detailed number of differnces should be counted" do
    expect(ScanReportPrinters::ScanSummaryReporter.new(nil, nil).only_totals).to be_truthy
    expect(ScanReportPrinters::ScanSummaryReporter.new(nil, "bla").only_totals).to be_truthy
    expect(ScanReportPrinters::ScanSummaryReporter.new(nil, "detailed").only_totals).to be_falsey
  end
  
  it "scan should count differences correctly in totals mode" do
    org_stdout = $stdout
    $stdout = StringIO.new
    begin
      reporter = ScanReportPrinters::ScanSummaryReporter.new(nil, nil)
      
      # set some existing scan result to ensure it gets reset before the next run
      reporter.scan_result = {:conflict => 0, :left => 0, :right => 1}
      
      reporter.scan('left_table', 'right_table') do 
        reporter.report_difference :conflict, :dummy_row
        reporter.report_difference :left, :dummy_row
        reporter.report_difference :right, :dummy_row
      end
      expect($stdout.string).to match(/left_table \/ right_table [\.\s]*3\n/)
    ensure 
      $stdout = org_stdout
    end
  end

  it "scan should count differences correctly in detailed mode" do
    org_stdout = $stdout
    $stdout = StringIO.new
    begin
      reporter = ScanReportPrinters::ScanSummaryReporter.new(nil, "detailed")
      
      reporter.scan('left_table', 'left_table') do
        reporter.report_difference :conflict, :dummy_row
        reporter.report_difference :left, :dummy_row
        reporter.report_difference :left, :dummy_row
        reporter.report_difference :right, :dummy_row
        reporter.report_difference :right, :dummy_row
        reporter.report_difference :right, :dummy_row
      end
      expect($stdout.string).to match(/left_table\s+1\s+2\s+3\n/)
    ensure 
      $stdout = org_stdout
    end
  end
end