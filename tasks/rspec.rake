begin
  require 'rspec'
rescue LoadError
  require 'rubygems'
  require 'rspec'
end

begin
  require 'rspec/core/rake_task'
rescue LoadError
  puts <<-EOS
To use rspec for testing you must install rspec gem:
    gem install rspec
  EOS
  exit(0)
end

desc "Run the specs under spec/models"
RSpec::Core::RakeTask.new do |t|
  t.rspec_opts = ['--options', "spec/spec.opts"]
  t.spec_files = FileList['spec/*_spec.rb']
end

namespace :spec do
  desc "Generate specdocs for examples for inclusion in RDoc"
  RSpec::Core::RakeTask.new('docs') do |t|
    t.spec_files = FileList['spec/*_spec.rb']
    t.rspec_opts = ["--format", "specdoc"]
  end

  desc "Run the specs with RCov"
  RSpec::Core::RakeTask.new('rcov') do |t|
    t.rspec_opts = ['--options', "spec/spec.opts"]
    t.spec_files = FileList['spec/*_spec.rb']
    t.rcov = true
    t.rcov_opts = [
      '--exclude', 'tasks/,spec/,gems/\(?!rubyrep\)',
      '--xrefs'
    ]
  end
  
  desc "Run the specs for all supported databases"
  task :all_dbs do
    [:postgres, :mysql].each do |test_db|
      puts "Running specs for #{test_db}"
      system "bash -c 'RR_TEST_DB=#{test_db} spec spec'"
    end
  end
  
  desc "Run the specs for all supported databases and ruby platforms" 
  task :all_rubies do
    system %(rvm exec bash -c 'for db in postgres mysql; do echo "`rvm current` - $db:"; RR_TEST_DB=$db spec spec; done')
  end
  
  begin
    require 'ruby-prof/task'
    RubyProf::ProfileTask.new do |t|
      t.test_files = FileList['spec/*_spec.rb']
      t.output_dir = 'profile'
      t.printer = :flat
      t.min_percent = 1
    end
  rescue LoadError
  end
end