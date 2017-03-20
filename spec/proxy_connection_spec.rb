require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ProxyConnection do
  before(:each) do
    Initializer.configuration = proxied_config
    @connection = ProxyConnection.new Initializer.configuration.left
  end

  it "initialize should connect to the database" do
    (!!@connection.connection.active?).should == true
  end

  it "initialize should store the configuratin" do
    @connection.config.should == Initializer.configuration.left
  end

  it "destroy should disconnect from the database" do
    if ActiveSupport.const_defined?(:Notifications)
      ConnectionExtenders::install_logger @connection.connection, :logger => StringIO.new
      log_subscriber = @connection.connection.log_subscriber

      ActiveSupport::Notifications.notifier.listeners_for("sql.active_record").should include(log_subscriber)
    end

    @connection.destroy

    if ActiveSupport.const_defined?(:Notifications)
      ActiveSupport::Notifications.notifier.listeners_for("sql.active_record").should_not include(log_subscriber)
      @connection.connection.log_subscriber.should be_nil
    end

    (!!@connection.connection.active?).should == false
  end

  it "cursors should return the current cursor hash or an empty hash if nil" do
    @connection.cursors.should == {}
    @connection.cursors[:dummy_cursor] = :dummy_cursor
    @connection.cursors.should == {:dummy_cursor => :dummy_cursor}
  end

  it "save_cursor should register the provided cursor" do
    @connection.save_cursor :dummy_cursor

    @connection.cursors[:dummy_cursor].should == :dummy_cursor
  end

  it "destroy should destroy and unregister any stored cursors" do
    cursor = mock("Cursor")
    cursor.should_receive(:destroy)

    @connection.save_cursor cursor
    @connection.destroy

    @connection.cursors.should == {}
  end

  it "destroy_cursor should destroy and unregister the provided cursor" do
    cursor = mock("Cursor")
    cursor.should_receive(:destroy)

    @connection.save_cursor cursor
    @connection.destroy_cursor cursor

    @connection.cursors.should == {}
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

      cursor.should be_an_instance_of(ProxyRowCursor)
      cursor.next_row_keys_and_checksum[0].should == {'id' => '2'} # verify that 'from' range was used
      cursor.next?.should be false # verify that 'to' range was used
    ensure
      @connection.execute('delete from scanner_records')
    end
  end

  it "column_names should return the column names of the specified table" do
    @connection.column_names('scanner_records').should == ['id', 'name']
  end

  it "column_names should cache the column names" do
    @connection.column_names('scanner_records')
    @connection.column_names('scanner_text_key')
    @connection.connection.should_not_receive(:columns)
    @connection.column_names('scanner_records').should == ['id', 'name']
  end

  it "primary_key_names should return the correct primary keys" do
    @connection.primary_key_names('scanner_records').should == ['id']
  end

  it "primary_key_names should return the manual primary keys if they exist" do
    @connection.stub!(:manual_primary_keys).
      and_return({'scanner_records' => ['manual_key']})
    @connection.primary_key_names('scanner_records').should == ['manual_key']
  end

  it "primary_key_names should not cache or manually overwrite if :raw option is given" do
    @connection.stub!(:manual_primary_keys).
      and_return({'scanner_records' => ['manual_key']})
    key1 = @connection.primary_key_names('scanner_records', :raw => true)
    key1.should == ['id']

    key2 = @connection.primary_key_names('scanner_records', :raw => true)
    key1.__id__.should_not == key2.__id__
  end

  it "primary_key_names should cache the primary primary keys" do
    @connection.connection.should_receive(:primary_key_names) \
      .with('dummy_table').once.and_return(['dummy_key'])
    @connection.connection.should_receive(:primary_key_names) \
      .with('dummy_table2').once.and_return(['dummy_key2'])

    @connection.primary_key_names('dummy_table').should == ['dummy_key']
    @connection.primary_key_names('dummy_table2').should == ['dummy_key2']
    @connection.primary_key_names('dummy_table').should == ['dummy_key']
  end

  # Note:
  # Additional select_cursor tests are executed via
  # 'db_specific_connection_extenders_spec.rb'
  # (To verify the behaviour for all supported databases)

  it "select_cursor should return the result fetcher" do
    fetcher = @connection.select_cursor(:table => 'scanner_records', :type_cast => false)
    fetcher.connection.should == @connection
    fetcher.options.should == {:table => 'scanner_records', :type_cast => false}
  end

  it "select_cursor should return a type casting cursor if :type_cast option is specified" do
    fetcher = @connection.select_cursor(:table => 'scanner_records', :type_cast => true)
    fetcher.should be_an_instance_of(TypeCastingCursor)
  end

  it "table_select_query should handle queries without any conditions" do
    @connection.table_select_query('scanner_records') \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records'\
        order by 'id'")
  end

  it "table_select_query should handle queries with only a from condition" do
    @connection.table_select_query('scanner_records', :from => {'id' => 1}) \
      .should =~ sql_to_regexp("\
         select 'id', 'name' from 'scanner_records' \
         where ('id') >= (1) order by 'id'")
  end

  it "table_select_query should handle queries with an exclusive from condition" do
    @connection.table_select_query(
      'scanner_records',
      :from => {'id' => 1},
      :exclude_starting_row => true
    ).should =~ sql_to_regexp("\
      select 'id', 'name' from 'scanner_records' \
      where ('id') > (1) order by 'id'")
  end

  it "table_select_query should handle queries with both from and to conditions" do
    @connection.table_select_query('scanner_records',
      :from => {'id' => 0}, :to => {'id' => 1}) \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where ('id') >= (0) and ('id') <= (1) order by 'id'")
  end

  it "table_select_query should handle queries for specific rows" do
    @connection.table_select_query('scanner_records',
      :row_keys => [{'id' => 0}, {'id' => 1}]) \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where ('id') in ((0), (1)) order by 'id'")
  end

  it "table_select_query should handle queries for specific rows with the row array actually being empty" do
    @connection.table_select_query('scanner_records', :row_keys => []) \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where false order by 'id'")
  end

  it "table_select_query should handle queries for specific rows in combination with other conditions" do
    @connection.table_select_query('scanner_records',
      :from => {'id' => 0},
      :row_keys => [{'id' => 1}, {'id' => 2}]) \
      .should =~ sql_to_regexp("\
        select 'id', 'name' from 'scanner_records' \
        where ('id') >= (0) and ('id') in ((1), (2)) order by 'id'")
  end

  it "table_insert_query should return the correct SQL query" do
    @connection.table_insert_query('scanner_records', 'name' => 'bla') \
      .should =~ sql_to_regexp(%q!insert into "scanner_records"("name") values("bla")!)
  end

  it "insert_record should insert the specified record" do
    begin
      @connection.insert_record('scanner_records', 'id' => 9, 'name' => 'bla')
      @connection.select_record(
        :table => 'scanner_records',
        :row_keys => ['id' => '9']
      ).should == {'id' => '9', 'name' => 'bla'}
    ensure
      @connection.execute('delete from scanner_records')
    end
  end

  it "insert_record should write nil values correctly" do
    begin
      @connection.insert_record('extender_combined_key', 'id' => 100, 'first_id' => 8, 'second_id' => '9', 'name' => nil)
      @connection.select_record(
        :table => 'extender_combined_key',
        :row_keys => ['id' => 100]
      ).should == { 'id' => '100', 'first_id' => '8', 'second_id' => '9', "name" => nil}
    ensure
      @connection.execute('delete from extender_combined_key')
    end
  end

  it "table_delete_query should return the correct SQL query" do
    @connection.table_delete_query('scanner_records', 'id' => 1) \
      .should =~ sql_to_regexp(%q!delete from "scanner_records" where ("id") = (1)!)
  end

  it "delete_record should delete the specified record" do
    begin
      @connection.delete_record('extender_combined_key', 'first_id' => 1, 'second_id' => '1', 'name' => 'xy')
      @connection.select_one(
        "select first_id, second_id, name
         from extender_combined_key where (first_id, second_id) = (1, 1)") \
        .should be_nil
    ensure
      @connection.execute('delete from extender_combined_key')
    end
  end

  it "delete_record should return the number of deleted records" do
    begin
      @connection.insert_record('extender_combined_key', { id: 100, first_id: 1, second_id: 1, name: 'aa' })
      @connection.
        delete_record('extender_combined_key', 'id' => 100).
        should == 1
      @connection.
        delete_record('extender_combined_key', 'first_id' => 1, 'second_id' => '0').
        should == 0
    ensure
      @connection.execute('delete from extender_combined_key')
    end
  end
end
