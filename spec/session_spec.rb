require File.dirname(__FILE__) + '/spec_helper.rb'
require 'yaml'

include RR

describe Session do # here database connection caching is _not_ disabled
  before(:each) do
    Initializer.configuration = standard_config
  end

  after(:each) do
  end

  it 'initialize should create (fake) proxy connections as per configuration' do
    dummy_proxy = Object.new
    dummy_connection = double('dummy connection')
    allow(dummy_connection).to receive(:tables).and_return([])
    allow(dummy_connection).to receive(:manual_primary_keys=)
    allow(dummy_connection).to receive(:select_one).and_return('x' => '2')
    expect(dummy_proxy).to receive(:create_session).and_return(dummy_connection)
    expect(DRbObject).to receive(:new).with(nil, 'druby://localhost:9876').and_return(dummy_proxy)

    session = Session.new proxied_config

    expect(session.proxies[:left]).to eq(dummy_proxy)
    expect(session.proxies[:right]).to be_an_instance_of(DatabaseProxy)

    expect(session.left).to eq(dummy_connection)
    expect(session.right).to be_an_instance_of(ProxyConnection)
  end

  it 'initialize should assign manual primary keys to the proxy connections' do
    config = deep_copy(standard_config)
    config.included_table_specs.clear
    config.include_tables 'table_with_manual_key, extender_without_key', primary_key_names: ['id']
    session = Session.new config
    expect(session.left.manual_primary_keys).to eq({ 'table_with_manual_key' => ['id'] })
    expect(session.right.manual_primary_keys).to eq({ 'extender_without_key' => ['id'] })
  end

  it 'refresh should raise error even if database connect fails silently' do
    session = Session.new
    session.right.destroy
    expect(session.right.connection).not_to be_active
    expect(session).to receive(:connect_database)
    expect { session.refresh }.to raise_error(/no connection to.*right.*database/)
  end

  it 'refresh should work with proxied database connections' do
    ensure_proxy
    session = Session.new(proxied_config)
    session.right.destroy
    expect(session.right.connection).not_to be_active
    expect { session.right.select_one('select 1+1 as x') }.to raise_error
    session.refresh
    expect(session.right.connection).to be_active
    expect(session.right.select_one('select 1+1 as x')['x'].to_i).to eq(2)
  end

  it 'disconnect_databases should disconnect both databases' do
    session = Session.new(standard_config)
    expect(session.left.connection).to be_active
    old_right_connection = session.right.connection
    expect(old_right_connection).to be_active
    session.disconnect_databases
    expect(session.left).to be_nil
    expect(session.right).to be_nil
    expect(old_right_connection).not_to be_active
  end

  it 'refresh should not do anyting if the connection is still active' do
    session = Session.new
    old_connection_id = session.right.connection.object_id
    session.refresh
    expect(session.right.connection.object_id).to eq(old_connection_id)
  end

  it 'refresh should replace active connections if forced is true' do
    session = Session.new
    old_connection_id = session.right.connection.object_id
    session.refresh forced: true
    expect(session.right.connection.object_id).not_to eq(old_connection_id)
  end

  it 'manual_primary_keys should return the specified manual primary keys' do
    config = deep_copy(standard_config)
    config.included_table_specs.clear
    config.include_tables 'table_with_manual_key, extender_without_key', key: ['id']
    session = Session.new config
    expect(session.manual_primary_keys(:left)).to eq({ 'table_with_manual_key' => ['id'] })
    expect(session.manual_primary_keys(:right)).to eq({ 'extender_without_key' => ['id'] })
  end

  it 'manual_primary_keys should accept keys that are not packed into an array' do
    config = deep_copy(standard_config)
    config.included_table_specs.clear
    config.include_tables 'table_with_manual_key', key: 'id'
    session = Session.new config
    expect(session.manual_primary_keys(:left)).to eq({ 'table_with_manual_key' => ['id'] })
  end

  it 'corresponding_table should return the correct corresponding table' do
    config = deep_copy(standard_config)
    config.included_table_specs.clear
    config.include_tables '/scanner/'
    config.include_tables 'table_with_manual_key, extender_without_key'
    session = Session.new config

    expect(session.corresponding_table(:left, 'scanner_records')).to eq('scanner_records')
    expect(session.corresponding_table(:right, 'scanner_records')).to eq('scanner_records')
    expect(session.corresponding_table(:left, 'table_with_manual_key')).to eq('extender_without_key')
    expect(session.corresponding_table(:right, 'extender_without_key')).to eq('table_with_manual_key')
  end

  it 'corresponding_table should return the given table if no corresponding table can be found' do
    session = Session.new
    expect(session.corresponding_table(:left, 'not_existing_table')).to eq('not_existing_table')
  end

  it 'configured_table_pairs should return the table pairs as per included_table_specs parameter' do
    session = Session.new
    expect(session.configured_table_pairs(['scanner_records'])).to eq([
      { left: 'scanner_records', right: 'scanner_records' }
    ])
  end

  def convert_table_array_to_table_pair_array(tables)
    tables.map { |table| { left: table, right: table } }
  end

  it 'sort_table_pairs should sort the tables correctly' do
    table_pairs = convert_table_array_to_table_pair_array(%w(
                                                            scanner_records
                                                            referencing_table
                                                            referenced_table
                                                            scanner_text_key
                                                          ))
    sorted_table_pairs = Session.new.sort_table_pairs(table_pairs)

    # ensure result holds the original table pairs
    p = proc { |l, r| l[:left] <=> r[:left] }
    expect(sorted_table_pairs.sort(&p)).to eq(table_pairs.sort(&p))

    # make sure the referenced table comes before the referencing table
    expect(sorted_table_pairs.map { |table_pair| table_pair[:left] }.grep(/referenc/))
                      .to eq(%w(referenced_table referencing_table))
  end

  it 'sort_table_pairs should not sort the tables if table_ordering is not enabled in the configuration' do
    table_pairs = convert_table_array_to_table_pair_array(%w(
                                                            scanner_records
                                                            referencing_table
                                                            referenced_table
                                                            scanner_text_key
                                                          ))
    config = deep_copy(standard_config)
    config.options[:table_ordering] = false
    session = Session.new config
    expect(session.sort_table_pairs(table_pairs)).to eq(table_pairs)
  end
end
