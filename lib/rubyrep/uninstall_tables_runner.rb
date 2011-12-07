$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'optparse'

module RR
  # This class implements the functionality of the 'uninstall_tables' command.
  class UninstallTablesRunner

    CommandRunner.register 'uninstall_tables' => {
      :command => self,
      :description => 'Removes all triggers, etc. from "left" and "right" database for specified tables'
    }

    # Provided options. Possible values:
    # * +:config_file+: path to config file
    attr_accessor :options

    # Parses the given command line parameter array.
    # Returns the status (as per UNIX conventions: 1 if parameters were invalid,
    # 0 otherwise)
    def process_options(args)
      status = 0
      self.options = {}

      parser = OptionParser.new do |opts|
        opts.banner = <<EOS
Usage: #{$0} uninstall_tables <table1 table2 ...> [options]

  Removes all triggers, etc. from "left" and "right" database for specified tables.
EOS
        opts.separator ""
        opts.separator "  Specific options:"

        opts.on("-t", "--tables", "=TABLES",
          "Mandatory. Comma separated list of tables to uninstall.") do |arg|
          options[:tables] = arg
        end

        opts.on("-c", "--config", "=CONFIG_FILE",
          "Mandatory. Path to configuration file.") do |arg|
          options[:config_file] = arg
        end

        opts.on_tail("--help", "Show this message") do
          $stderr.puts opts
          self.options = nil
        end
      end

      begin
        parser.parse!(args)
        if options # this will be +nil+ if the --help option is specified
          raise("Please specify configuration file") unless options.include?(:config_file)
          raise("Please specify tables") unless options.include?(:tables)
        end
      rescue Exception => e
        $stderr.puts "Command line parsing failed: #{e}"
        $stderr.puts parser.help
        self.options = nil
        status = 1
      end

      return status
    end

    # Returns the active +Session+.
    # Loads config file and creates session if necessary.
    def session
      unless @session
        load options[:config_file]
        @session = Session.new Initializer.configuration
      end
      @session
    end

    # Removes all rubyrep created database objects.
    def execute
      initializer = ReplicationInitializer.new session
      initializer.restore_configured_tables(options[:tables].split(",").map { |t| {:left => t, :right => t} })
    end

    # Entry points for executing a processing run.
    # args: the array of command line options that were provided by the user.
    def self.run(args)
      runner = new

      status = runner.process_options(args)
      if runner.options
        runner.execute
      end
      status
    end

  end
end


