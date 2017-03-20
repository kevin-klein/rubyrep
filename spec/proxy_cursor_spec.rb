require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ProxyCursor do
  before(:each) do
    Initializer.configuration = proxied_config
  end

  it "initialize should store session and table and cache the primary keys of table" do
    connection = create_mock_proxy_connection 'dummy_table', ['dummy_key']

    cursor = ProxyCursor.new connection, 'dummy_table'

    expect(cursor.connection).to eq(connection)
    expect(cursor.table).to eq('dummy_table')
    expect(cursor.primary_key_names).to eq(['dummy_key'])
  end

  it "prepare_fetch should initiate the query and wrap it for type casting" do
    connection = ProxyConnection.new Initializer.configuration.left

    connection.execute('delete from scanner_records')

    connection.insert_record('scanner_records', {
      id: 1,
      name: 'Alice - exists in both databases'
    })

    cursor = ProxyCursor.new(connection, 'scanner_records')
    cursor.prepare_fetch
    expect(cursor.cursor).to be_an_instance_of(TypeCastingCursor)
    expect(cursor.cursor.next_row).to eq({'id' => '1', 'name' => 'Alice - exists in both databases'})

    connection.execute('delete from scanner_records')
  end


  it "destroy should clear and nil the cursor" do
    connection = create_mock_proxy_connection 'dummy_table', ['dummy_key']
    cursor = ProxyCursor.new connection, 'dummy_table'

    table_cursor = double("DBCursor")
    expect(table_cursor).to receive(:clear)
    cursor.cursor = table_cursor

    cursor.destroy
    expect(cursor.cursor).to be_nil
  end
end
