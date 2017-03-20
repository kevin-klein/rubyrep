require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ScanReportPrinters do
  before(:each) do
  end

  it "report_printers should an empty array if there are no registered printers" do
    org_printers = ScanReportPrinters.printers
    begin
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, nil }
      expect(ScanReportPrinters.printers).to eq([])
    ensure
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, org_printers }
    end
  end
  
  it "register_printer should store the provided printers, report_printer should return them" do
    org_printers = ScanReportPrinters.printers
    begin
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, nil }
      ScanReportPrinters.register :dummy_printer_class, "-d", "--dummy"
      ScanReportPrinters.register :another_printer_class, "-t"
      expect(ScanReportPrinters.printers).to eq([
        { :printer_class => :dummy_printer_class,
          :opts => ["-d", "--dummy"]
        },
        { :printer_class => :another_printer_class,
          :opts => ["-t"]
        }
      ])
    ensure
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, org_printers }
    end
  end

  it "on_printer_selection should create and yield the printer class and option argument" do
    org_printers = ScanReportPrinters.printers
    begin
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, nil }

      # register a printer class which will not be selected in the command line options
      printer_x = double("printer_x")
      ScanReportPrinters.register printer_x, "-x", "--printer_x"

      # register a printer class that will be selected in the command line options
      printer_y = double("printer_y")

      ScanReportPrinters.register printer_y, "-y", "--printer_y[=arg]", "description"

      selected_printer_class = nil
      selected_arg = nil
      parser = OptionParser.new
      ScanReportPrinters.on_printer_selection(parser) do |printer_class, arg|
        selected_printer_class = printer_class
        selected_arg = arg
      end
      parser.parse!(["--printer_y=dummy_arg"])

      expect(selected_printer_class).to eq(printer_y)
      expect(selected_arg).to eq('dummy_arg')
    ensure
      ScanReportPrinters.instance_eval { class_variable_set :@@report_printers, org_printers }
    end
  end
end