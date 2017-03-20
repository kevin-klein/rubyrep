require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ProxyConnection do
  before(:each) do
    Initializer.configuration = proxied_config
    @connection = ProxyConnection.new Initializer.configuration.left
  end

  it "initialize should connect to the database" do
    expect(!!@connection.connection.active?).to eq(true)
  end

  it "initialize should store the configuratin" do
    expect(@connection.config).to eq(Initializer.configuration.left)
  end

  it "destroy should disconnect from the database" do
    if ActiveSupport.const_defined?(:Notifications)
      ConnectionExtenders::install_logger @connection.connection, :logger => StringIO.new
      log_subscriber = @connection.connection.log_subscriber

      expect(ActiveSupport::Notifications.notifier.listeners_for("sql.active_record")).to include(log_subscriber)
    end

    @connection.destroy

    if ActiveSupport.const_defined?(:Notifications)
      expect(ActiveSupport::Notifications.notifier.listeners_for("sql.active_record")).not_to include(log_subscriber)
      expect(@connection.connection.log_subscriber).to be_nil
    end

    expect(!!@connection.connection.active?).to eq(false)
  end

  it "cursors should return the current cursor hash or an empty hash if nil" do
    expect(@connection.cursors).to eq({})
    @connection.cursors[:dummy_cursor] = :dummy_cursor
    expect(@connection.cursors).to eq({:dummy_cursor => :dummy_cursor})
  end

  it "save_cursor should register the provided cursor" do
    @connection.save_cursor :dummy_cursor

    expect(@connection.cursors[:dummy_cursor]).to eq(:dummy_cursor)
  end

  it "destroy should destroy and unregister any stored cursors" do
    cursor = double("Cursor")
    expect(cursor).to receive(:destroy)

    @connection.save_cursor cursor
    @connection.destroy

    expect(@connection.cursors).to eq({})
  end

  it "destroy_cursor should destroy and unregister the provided cursor" do
    cursor = double("Cursor")
    expect(cursor).to receive(:destroy)

    @connection.save_cursor cursor
    @connection.destroy_cursor cursor

    expect(@connection.cursors).to eq({})
  end

  it "create_cursor should create and register the cursor and initiate row fetching" do
    begin
      cursor = @connection.create_cursor(
        ProxyRowCursor,
        'scanner_records',
        :from => {'id' => 2},
        :to => {'id' => 2}
      )

      @connection.insert_record('scanner_records', {
        'id' => 2,
        'name' => 'Text'
      })

      expect(cursor).to be_an_instance_of(ProxyRowCursor)
      expect(cursor.next_row_keys_and_checksum[0]).to eq({'id' => '2'}) # verify that 'from' range was used
      expect(cursor.next?).to be false # verify that 'to' range was used
    ensure
      @connection.execute('delete from scanner_records')
    end
  end

  it "column_names should return the column names of the specified table" do
    expect(@connection.column_names('scanner_records')).to eq(['id', 'name'])
  end

  it "column_names should cache the column names" do
    @connection.column_names('scanner_records')
    @connection.column_names('scanner_text_key')
    expect(@connection.connection).not_to receive(:columns)
    expect(@connection.column_names('scanner_records')).to eq(['id', 'name'])
  end

  it "primary_key_names should return the correct primary keys" do
    expect(@connection.primary_key_names('scanner_records')).to eq(['id'])
  end

  it "primary_key_names should return the manual primary keys if they exist" do
    allow(@connection).to receive(:manual_primary_keys).
      and_return({'scanner_records' => ['manual_key']})
    expect(@connection.primary_key_names('scanner_records')).to eq(['manual_key'])
  end

  it "primary_key_names should not cache or manually overwrite if :raw option is given" do
    allow(@connection).to receive(:manual_primary_keys).
      and_return({'scanner_records' => ['manual_key']})
    key1 = @connection.primary_key_names('scanner_records', :raw => true)
    expect(key1).to eq(['id'])

    key2 = @connection.primary_key_names('scanner_records', :raw => true)
    expect(key1.__id__).not_to eq(key2.__id__)
  end

  it "primary_key_names should cache the primary primary keys" do
    expect(@connection.connection).to receive(:primary_key_names) \
      .with('dummy_table').once.and_return(['dummy_key'])
    expect(@connection.connection).to receive(:primary_key_names) \
      .with('dummy_table2').once.and_return(['dummy_key2'])

    expect(@connection.primary_key_names('dummy_table')).to eq(['dummy_key'])
    expect(@connection.primary_key_names('dummy_table2')).to eq(['dummy_key2'])
    expect(@connection.primary_key_names('dummy_table')).to eq(['dummy_key'])
  end

  # Note:
  # Additional select_cursor tests are executed via
  # 'db_specific_connection_extenders_spec.rb'
  # (To verify the behaviour for all supported databases)

  it "select_cursor should return the result fetcher" do
    fetcher = @connection.select_cursor(:table => 'scanner_records', :type_cast => false)
    expect(fetcher.connection).to eq(@connection)
    expect(fetcher.options).to eq({:table => 'scanner_records', :type_cast => false})
  end

  it "select_cursor should return a type casting cursor if :type_cast option is specified" do
    fetcher = @connection.select_cursor(:table => 'scanner_records', :type_cast => true)
    expect(fetcher).to be_an_instance_of(TypeCastingCursor)
  end

  it "table_select_query should handle queries without any conditions" do
    expect(@connection.table_select_query('scanner_records')) \
      .to match(sql_to_regexp("\
        select 'id', 'name' from 'scanner_records'\
        order by 'id'"))
  end

  it "table_select_query should handle queries with only a from condition" do
    expect(@connection.table_select_query('scanner_records', :from => {'id' => 1})) \
      .to match(sql_to_regexp("\
         select 'id', 'name' from 'scanner_records' \
         where ('id') >= (1) order by 'id'"))
  end

  it "table_select_query should handle queries with an exclusive from condition" do
    expect(@connection.table_select_query(
      'scanner_records',
      :from => {'id' => 1},
      :exclude_starting_row => true
    )).to match(sql_to_regexp("\
      select 'id', 'name' from 'scanner_records' \
      where ('id') > (1) order by 'id'"))
  end

  it "table_select_query should handle queries with both from and to conditions" do
    expect(@connection.table_select_query('scanner_records',
      :from => {'id' => 0}, :to => {'id' => 1})) \
      .to match(sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where ('id') >= (0) and ('id') <= (1) order by 'id'"))
  end

  it "table_select_query should handle queries for specific rows" do
    expect(@connection.table_select_query('scanner_records',
      :row_keys => [{'id' => 0}, {'id' => 1}])) \
      .to match(sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where ('id') in ((0), (1)) order by 'id'"))
  end

  it "table_select_query should handle queries for specific rows with the row array actually being empty" do
    expect(@connection.table_select_query('scanner_records', :row_keys => [])) \
      .to match(sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where false order by 'id'"))
  end

  it "table_select_query should handle queries for specific rows in combination with other conditions" do
    expect(@connection.table_select_query('scanner_records',
      :from => {'id' => 0},
      :row_keys => [{'id' => 1}, {'id' => 2}])) \
      .to match(sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where ('id') >= (0) and ('id') in ((1), (2)) order by 'id'"))
  end

  it "table_insert_query should return the correct SQL query" do
    expect(@connection.table_insert_query('scanner_records', 'name' => 'bla')) \
      .to match(sql_to_regexp(%q!insert into "scanner_records"("name") values("bla")!))
  end

  it "insert_record should insert the specified record" do
    begin
      @connection.insert_record('scanner_records', 'id' => 9, 'name' => 'bla')
      expect(@connection.select_record(
        :table => 'scanner_records',
        :row_keys => ['id' => '9']
      )).to eq({'id' => '9', 'name' => 'bla'})
    ensure
      @connection.execute('delete from scanner_records')
    end
  end

  it "insert_record should write nil values correctly" do
    begin
      @connection.insert_record('extender_combined_key', 'id' => 100, 'first_id' => 8, 'second_id' => '9', 'name' => nil)
      expect(@connection.select_record(
        :table => 'extender_combined_key',
        :row_keys => ['id' => 100]
      )).to eq({ 'id' => '100', 'first_id' => '8', 'second_id' => '9', "name" => nil})
    ensure
      @connection.execute('delete from extender_combined_key')
    end
  end

  it "table_delete_query should return the correct SQL query" do
    expect(@connection.table_delete_query('scanner_records', 'id' => 1)) \
      .to match(sql_to_regexp(%q!delete from "scanner_records" where ("id") = (1)!))
  end

  it "delete_record should delete the specified record" do
    begin
      @connection.delete_record('extender_combined_key', 'first_id' => 1, 'second_id' => '1', 'name' => 'xy')
      expect(@connection.select_one(
        "select first_id, second_id, name
         from extender_combined_key where (first_id, second_id) = (1, 1)")) \
        .to be_nil
    ensure
      @connection.execute('delete from extender_combined_key')
    end
  end

  it "delete_record should return the number of deleted records" do
    begin
      @connection.insert_record('extender_combined_key', { id: 100, first_id: 1, second_id: 1, name: 'aa' })
      expect(@connection.
        delete_record('extender_combined_key', 'id' => 100)).
        to eq(1)
      expect(@connection.
        delete_record('extender_combined_key', 'first_id' => 1, 'second_id' => '0')).
        to eq(0)
    ensure
      @connection.execute('delete from extender_combined_key')
    end
  end
end
