require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ProxiedTableScan do
  before(:each) do
    Initializer.configuration = deep_copy(proxied_config)

    # Small block size necessary to exercize all code paths in ProxiedTableScan
    # even when only using tables with very small number of records.
    Initializer.configuration.options[:proxy_block_size] = 2

    ensure_proxy

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

  it "initialize should raise exception if session is not proxied" do
    session = Session.new standard_config
    expect { ProxiedTableScan.new session, 'dummy_table' } \
      .to raise_error(RuntimeError, /only works with proxied sessions/)
  end

  it "initialize should cache the primary keys" do
    session = Session.new
    scan = ProxiedTableScan.new session, 'scanner_records'
    expect(scan.primary_key_names).to eq(['id'])
  end

  it "block_size should return the :proxy_block_size value of the session options" do
    expect(ProxiedTableScan.new(Session.new, 'scanner_records').block_size) \
      .to eq(2)
  end

  it "block_size should return the matching table specific option if available" do
    config = Initializer.configuration
    old_table_specific_options = config.tables_with_options
    begin
      config.options = {:proxy_block_size => 2}
      config.include_tables 'scanner_records', {:proxy_block_size => 3}
      expect(ProxiedTableScan.new(Session.new(config), 'scanner_records').block_size) \
        .to eq(3)
    ensure
      config.instance_eval {@tables_with_options = old_table_specific_options}
    end
  end

  # Creates, prepares and returns a +ProxyBlockCursor+ for the given database
  # +connection+ and +table+.
  # Sets the ProxyBlockCursor#max_row_cache_size as per method parameter.
  def get_block_cursor(connection, table, max_row_cache_size = 1000000)
    cursor = ProxyBlockCursor.new connection, table
    cursor.max_row_cache_size = max_row_cache_size
    cursor.prepare_fetch
    cursor.checksum :proxy_block_size => 1000
    cursor
  end

  it "compare_blocks should compare all the records in the range" do
    session = Session.new

    left_cursor = get_block_cursor session.left, 'scanner_records'
    right_cursor = get_block_cursor session.right, 'scanner_records'

    scan = ProxiedTableScan.new session, 'scanner_records'
    diff = []
    scan.compare_blocks(left_cursor, right_cursor) do |type, row|
      diff.push [type, row]
    end
    # in this scenario the right table has the 'highest' data,
    # so 'right-sided' data are already implicitely tested here
    expect(diff).to eq([[:conflict, [{"id"=>'2', "name"=>"Bob - left database version"}, {"id"=>'2', "name"=>"Bob - right database version"}]], [:left, {"id"=>'3', "name"=>"Charlie - exists in left database only"}], [:right, {"id"=>'4', "name"=>"Dave - exists in right database only"}], [:left, {"id"=>'5', "name"=>"Eve - exists in left database only"}], [:right, {"id"=>'6', "name"=>"Fred - exists in right database only"}]])
  end

  it "compare_blocks should destroy the created cursors" do
    session = Session.new

    left_cursor = get_block_cursor session.left, 'scanner_records', 0
    right_cursor = get_block_cursor session.right, 'scanner_records', 0

    scan = ProxiedTableScan.new session, 'scanner_records'
    scan.compare_blocks(left_cursor, right_cursor) { |type, row| }

    expect(session.left.cursors).to eq({})
    expect(session.right.cursors).to eq({})
  end

  it "run should only call compare single rows if there are different block checksums" do
    config = deep_copy(proxied_config)
    config.right = config.left
    session = Session.new config
    scan = ProxiedTableScan.new session, 'scanner_records'
    expect(scan).not_to receive(:compare_blocks)
    diff = []
    scan.run do |type, row|
      diff.push [type,row]
    end
    expect(diff).to eq([])
  end

  it "run should compare all the records in the table" do
    session = Session.new
    scan = ProxiedTableScan.new session, 'scanner_records'
    diff = []
    scan.run do |type, row|
      diff.push [type, row]
    end
    # in this scenario the right table has the 'highest' data,
    # so 'right-sided' data are already implicitely tested here
    expect(diff).to eq([[:conflict, [{"id"=>'2', "name"=>"Bob - left database version"}, {"id"=>'2', "name"=>"Bob - right database version"}]], [:left, {"id"=>'3', "name"=>"Charlie - exists in left database only"}], [:right, {"id"=>'4', "name"=>"Dave - exists in right database only"}], [:left, {"id"=>'5', "name"=>"Eve - exists in left database only"}], [:right, {"id"=>'6', "name"=>"Fred - exists in right database only"}]])
  end

  it "run should update the progress" do
    session = Session.new
    scan = ProxiedTableScan.new session, 'scanner_records'
    number_steps = 0
    allow(scan).to receive(:update_progress) do |steps|
      number_steps += steps
    end
    scan.run {|_, _|}
    expect(number_steps).to eq(6)
  end

  it "run should update the progress even if there are no records" do
    # it should do that to ensure the progress bar is printed
    scan = ProxiedTableScan.new Session.new, 'extender_no_record'
    expect(scan).to receive(:update_progress).at_least(:once)
    scan.run {|_, _|}
  end
end
