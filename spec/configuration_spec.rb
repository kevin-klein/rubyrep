require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Configuration do
  before(:each) do
  end

  it "initialize should set #left and #right to empty hashes" do
    config = Configuration.new
    expect(config.left).to eq({})
    expect(config.right).to eq({})
  end
  
  it "initialize should set #options to the default options" do
    config = Configuration.new
    expect(config.options).to eq(Configuration::DEFAULT_OPTIONS)
  end

  it "options= should merge the provided into the existing options" do
    config = Configuration.new
    config.options = {:bla => :bla}
    config.options = {:bla => :blub}
    expect(config.options[:bla]).to eq(:blub)
  end

  it "each_matching_option should yield if general option matches" do
    config = Configuration.new
    config.options = {:bla => :blub}
    yielded = []
    config.each_matching_option(:bla) {|spec, value| yielded << [spec, value]}
    expect(yielded).to eq([[nil, :blub]])
  end

  it "each_matching_option should yield if table specific options match" do
    config = Configuration.new
    config.options = {:a => 1}
    config.add_table_options 't1', :a => 2
    config.add_table_options 't2', :b => 3
    config.add_table_options 't3', :a => 4
    yielded = []
    config.each_matching_option(:a) {|spec, value| yielded << [spec, value]}
    expect(yielded).to eq([
      [nil,  1],
      ['t1', 2],
      ['t3', 4]
    ])
  end

  it "each_matching_option should not yield unmatching options" do
    config = Configuration.new
    config.options = {:a => :blub}
    config.add_table_options 'dummy_table', :b => :blub
    yielded = []
    config.each_matching_option(:c) {|spec, value| yielded << [spec, value]}
    expect(yielded).to eq([])
  end

  it "options_for_table should return the general options if there are no table specific options at all" do
    config = Configuration.new
    expect(config.options_for_table('b')).to eq( \
      Syncers::TwoWaySyncer.default_options.clone.
      merge(Replicators::TwoWayReplicator.default_options.clone).
      merge(config.options)
    )
  end

  it "included_table_specs should return the list of included table specifications" do
    config = Configuration.new
    config.include_tables('a', {:bla => :blub})
    config.include_tables('a, b')
    config.include_tables(/a/)
    expect(config.included_table_specs).to eq(['a', 'a, b', /a/])
  end

  it "included_table_specs should save the options if provided" do
    config = Configuration.new
    config.include_tables('a', {:bla => :blub})
    expect(config.options_for_table('a')[:bla]).to eq(:blub)
  end

  it "include_tables should include the specified table specs" do
    config = Configuration.new
    config.include_tables('a')
    config.include_tables(/b/)
    expect(config.included_table_specs.include?('a')).to be_truthy
    expect(config.included_table_specs.include?(/b/)).to be_truthy
  end

  it "include_table should alias to include_tables" do
    config = Configuration.new
    config.include_table('a')
    expect(config.included_table_specs.include?('a')).to be_truthy
  end

  it "exclude_tables should exclude the specified table specs" do
    config = Configuration.new
    config.exclude_tables('a')
    config.exclude_tables(/b/)
    expect(config.excluded_table_specs.include?('a')).to be_truthy
    expect(config.excluded_table_specs.include?(/b/)).to be_truthy
  end

  it "exclude_table should alias to exclude_tables" do
    config = Configuration.new
    config.exclude_table('a')
    expect(config.excluded_table_specs.include?('a')).to be_truthy
  end

  it "exclude_rubyrep_tables should exclude the rubyrep infrastructure tables" do
    config = Configuration.new
    config.exclude_rubyrep_tables
    expect(config.excluded_table_specs.include?(/^rr_.*/)).to be_truthy
  end

  it "excluded_table_specs should return the list of excluded table specifications" do
    config = Configuration.new
    config.exclude_tables('a')
    config.exclude_tables('a, b')
    config.exclude_tables(/a/)
    expect(config.excluded_table_specs).to eq(['a', 'a, b', /a/])
  end

  it "options_for_table should return the general options if there are no matching table specific options" do
    config = Configuration.new
    config.include_tables(/a/, {:bla => :blub})
    expect(config.options_for_table('b')).to eq( \
      Syncers::TwoWaySyncer.default_options.clone.
      merge(Replicators::TwoWayReplicator.default_options.clone).
      merge(config.options)
    )
  end

  it "options_for_table should return table specific options mixed in with default options" do
    config = Configuration.new
    config.include_tables(/a/, {:bla => :blub})
    expect(config.options_for_table('a')).to eq( \
      Syncers::TwoWaySyncer.default_options.clone.
      merge(Replicators::TwoWayReplicator.default_options.clone).
      merge(config.options).
      merge(:bla => :blub)
    )
  end

  it "options_for_table should return last added version of added options for matching table spec" do
    config = Configuration.new
    config.include_tables(/a/, {:bla => :blub})
    config.include_tables('a', {:bla => :blok})
    config.include_tables(/x/, {:bla => :bar})
    config.include_tables('y', {:bla => :foo})
    expect(config.options_for_table('a')).to eq( \
      Syncers::TwoWaySyncer.default_options.clone.
      merge(Replicators::TwoWayReplicator.default_options.clone).
      merge(config.options).
      merge(:bla => :blok)
    )
  end

  it "options_for_table should match against table pair specs" do
    config = Configuration.new
    config.add_table_options('a, b', {:bla => :blub})
    expect(config.options_for_table('a')[:bla]).to eq(:blub)
  end

  it "options_for_table should match against regular expression specs" do
    config = Configuration.new
    config.add_table_options(/a/, {:bla => :blub})
    expect(config.options_for_table('a')[:bla]).to eq(:blub)
  end

  it "options_for_table should match against pure table name specs" do
    config = Configuration.new
    config.add_table_options('a', {:bla => :blub})
    expect(config.options_for_table('a')[:bla]).to eq(:blub)
  end

  it "add_table_options should not create table_spec duplicates" do
    config = Configuration.new
    config.add_table_options(/a/, {:bla => :blub})
    config.add_table_options(/a/, {:foo => :bar})
    expect(config.options_for_table('a')).to eq( \
      Syncers::TwoWaySyncer.default_options.clone.
      merge(Replicators::TwoWayReplicator.default_options.clone).
      merge(config.options).
      merge(:bla => :blub, :foo => :bar)
    )
  end

  it "add_table_option should alias to add_table_options" do
    config = Configuration.new
    config.add_table_option(/a/, {:bla => :blub})
    expect(config.options_for_table('a')[:bla]).to eq(:blub)
  end

  it "add_table_options should include default syncer options" do
    config = Configuration.new
    config.options = {:syncer => :one_way}

    # overwrite one syncer option
    config.add_table_options(/a/, {:delete => true})

    options = config.options_for_table('a')
    Syncers::OneWaySyncer.default_options.each do |key, value|
      expect(options[key]).to eq(value) unless key == :delete
    end
    expect(options[:delete]).to eq(true)
  end
end