require File.dirname(__FILE__) + '/spec_helper.rb'
require File.dirname(__FILE__) + "/../config/test_config.rb"

include RR

describe BaseRunner do
  before(:each) do
  end

  it "process_options should make options as nil and teturn status as 1 if command line parameters are unknown" do
    # also verify that an error message is printed
    allow($stderr).to receive(:puts)
    runner = BaseRunner.new
    status = runner.process_options ["--nonsense"]
    expect(runner.options).to eq(nil)
    expect(status).to eq(1)
  end

  it "process_options should make options as nil and return status as 1 if config option is not given" do
    # also verify that an error message is printed
    allow($stderr).to receive(:puts)
    runner = BaseRunner.new
    status = runner.process_options ["table"]
    expect(runner.options).to eq(nil)
    expect(status).to eq(1)
  end

  it "process_options should show the summary description (if usage is printed)" do
    org_stderr = $stderr
    $stderr = StringIO.new
    begin
      base_runner = BaseRunner.new
      expect(base_runner).to receive(:summary_description).
        and_return("my_summary_description")
      base_runner.process_options ["--help"]
      expect($stderr.string).to match(/my_summary_description/)
    ensure
      $stderr = org_stderr
    end
  end

  it "process_options should make options as nil and return status as 0 if command line includes '--help'" do
    # also verify that the help message is printed
    expect($stderr).to receive(:puts)
    runner = BaseRunner.new
    status = runner.process_options ["--help"]
    expect(runner.options).to eq(nil)
    expect(status).to eq(0)
  end

  it "process_options should set the correct options" do
    runner = BaseRunner.new
    runner.process_options ["-c", "config_path", "table_spec1", "table_spec2"]
    expect(runner.options[:config_file]).to eq('config_path')
    expect(runner.options[:table_specs]).to eq(['table_spec1', 'table_spec2'])
  end

  it "process_options should add runner specific options" do
    BaseRunner.any_instance_should_receive(:add_specific_options) do
      runner = BaseRunner.new
      runner.process_options ["-c", "config_path"]
    end
  end

  it "process_options should assign the command line specified report printer" do
    org_printers = ScanReportPrinters.printers
    begin
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, nil }

      ScanReportPrinters.register :dummy_printer_class, "-y", "--printer_y[=arg]", "description"

      runner = BaseRunner.new
      allow(runner).to receive(:session)
      runner.process_options ["-c", "config_path", "--printer_y=arg_for_y", "table_spec"]
      expect(runner.report_printer_class).to eq(:dummy_printer_class)
      expect(runner.report_printer_arg).to eq('arg_for_y')
    ensure
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, org_printers }
    end
  end

  it "process_options should assign the command line specified progress printer class" do
    org_printers = ScanProgressPrinters.printers
    begin
      ScanProgressPrinters.instance_eval { class_variable_set :@@progress_printers, nil }

      printer_y_class = double("printer_y_class")
      expect(printer_y_class).to receive(:arg=)

      ScanProgressPrinters.register :printer_y_key, printer_y_class, "-y", "--printer_y[=arg]", "description"

      runner = BaseRunner.new
      runner.process_options ["-c", "config_path", "-y", "arg_for_y"]
      expect(runner.progress_printer).to eq(printer_y_class)
    ensure
      ScanProgressPrinters.instance_eval { class_variable_set :@@progress_printers, org_printers }
    end
  end

  it "add_specific_options should not do anything" do
    BaseRunner.new.add_specific_options nil
  end

  it "create_processor should not do anything" do
    BaseRunner.new.create_processor "dummy_left_table", "dummy_right_table"
  end

  it "prepare_table_pairs should return the provided table pairs unmodied" do
    expect(BaseRunner.new.prepare_table_pairs(:dummy_table_pairs)).
      to eq(:dummy_table_pairs)
  end

  it "run should not start a scan if the command line is invalid" do
    allow($stderr).to receive(:puts)
    BaseRunner.any_instance_should_not_receive(:execute) {
      BaseRunner.run(["--nonsense"])
    }
  end

  it "run should start a scan if the command line is correct" do
    BaseRunner.any_instance_should_receive(:execute) {
      BaseRunner.run(["--config=path", "table"])
    }
  end

  it "report_printer should create and return the printer as specified per command line options" do
    printer_class = double("printer class")
    expect(printer_class).to receive(:new).with(:dummy_session, :dummy_arg).and_return(:dummy_printer)
    runner = BaseRunner.new
    allow(runner).to receive(:session).and_return(:dummy_session)
    runner.report_printer_class = printer_class
    runner.report_printer_arg = :dummy_arg
    expect(runner.report_printer).to eq(:dummy_printer)
    runner.report_printer # ensure the printer object is cached
  end

  it "report_printer should return the ScanSummaryReporter if no other printer was chosen" do
    runner = BaseRunner.new
    allow(runner).to receive(:session)
    expect(runner.report_printer).to be_an_instance_of(ScanReportPrinters::ScanSummaryReporter)
  end

  it "progress_printer should return the config file specified printer if none was give via command line" do
    runner = BaseRunner.new
    runner.options = {
      :config_file => "#{File.dirname(__FILE__)}/../config/test_config.rb",
      :table_specs => ["scanner_records", "extender_one_record"]
    }
    config_specified_printer_key = Session.new(standard_config).configuration.options[:scan_progress_printer]
    config_specified_printer_class = ScanProgressPrinters.
      printers[config_specified_printer_key][:printer_class]
    expect(runner.progress_printer).to eq(config_specified_printer_class)
  end

  it "signal_scanning_completion should signal completion if the scan report printer supports it" do
    printer = double("printer")
    expect(printer).to receive(:scanning_finished)
    expect(printer).to receive(:respond_to?).with(:scanning_finished).and_return(true)
    runner = BaseRunner.new
    allow(runner).to receive(:report_printer).and_return(printer)
    runner.signal_scanning_completion
  end

  it "signal_scanning_completion should not signal completion if the scan report printer doesn't supports it" do
    printer = double("printer")
    expect(printer).not_to receive(:scanning_finished)
    expect(printer).to receive(:respond_to?).with(:scanning_finished).and_return(false)
    runner = BaseRunner.new
    allow(runner).to receive(:report_printer).and_return(printer)
    runner.signal_scanning_completion
  end

  it "execute should process the specified tables" do
    org_stdout = $stdout
    $stdout = StringIO.new
    begin
      runner = BaseRunner.new
      runner.options = {
        :config_file => "#{File.dirname(__FILE__)}/../config/test_config.rb",
        :table_specs => ["scanner_records", "extender_one_record"]
      }

      # create and install a dummy processor
      processor = double("dummy_processor")
      expect(processor).to receive(:run).twice.and_yield(:left, :dummy_row)

      # verify that the scanner receives the progress printer
      allow(runner).to receive(:progress_printer).and_return(:dummy_printer_class)
      expect(processor).to receive(:progress_printer=).twice.with(:dummy_printer_class)

      expect(runner).to receive(:create_processor).twice.and_return(processor)

      # verify that the scanning_completion signal is given to scan report printer
      expect(runner).to receive :signal_scanning_completion

      runner.execute

      # verify that rubyrep infrastructure tables were excluded
      expect(runner.session.configuration.excluded_table_specs.include?(/^rr_.*/)).to be_truthy

      expect($stdout.string).to match(/scanner_records.* 1\n/)
      expect($stdout.string).to match(/extender_one_record.* 1\n/)
    ensure
      $stdout = org_stdout
    end
  end

  it "table_pairs should return the prepared table pairs" do
    runner = BaseRunner.new
    runner.options = {
      :config_file => "#{File.dirname(__FILE__)}/../config/test_config.rb",
      :table_specs => ['scanner_records']
    }
    expect(runner).to receive(:prepare_table_pairs).with([
      {:left => 'scanner_records', :right => 'scanner_records'},
    ]).and_return(:dummy_table_pairs)
    expect(runner.table_pairs).to eq(:dummy_table_pairs)
  end
end
