require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ProxyRowCursor do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it 'initialize should super to ProxyCursor' do
    session = create_mock_proxy_connection 'dummy_table', ['dummy_id']
    cursor = ProxyRowCursor.new session, 'dummy_table'
    expect(cursor.table).to eq('dummy_table')
  end

  it 'next? should delegate to the DB cursor' do
    session = create_mock_proxy_connection 'dummy_table', ['dummy_id']
    cursor = ProxyRowCursor.new session, 'dummy_table'

    table_cursor = double('DBCursor')
    expect(table_cursor).to receive(:next?).and_return(true)
    cursor.cursor = table_cursor

    expect(cursor.next?).to eq(true)
  end

  it 'next_row should return the next row in the cursor' do
    session = create_mock_proxy_connection 'dummy_table', ['dummy_id']
    cursor = ProxyRowCursor.new session, 'dummy_table'

    table_cursor = double('DBCursor')
    expect(table_cursor).to receive(:next_row).and_return(:dummy_row)
    cursor.cursor = table_cursor

    expect(cursor.next_row).to eq(:dummy_row)
  end

  it 'next_row_keys_and_checksum should store the found row under current_row' do
    session = create_mock_proxy_connection 'dummy_table', ['dummy_id']
    cursor = ProxyRowCursor.new session, 'dummy_table'

    table_cursor = double('DBCursor')
    expect(table_cursor).to receive(:next_row).and_return('dummy_id' => 'dummy_value')

    cursor.cursor = table_cursor
    cursor.next_row_keys_and_checksum
    expect(cursor.current_row).to eq({ 'dummy_id' => 'dummy_value' })
  end

  it 'next_row_keys_and_checksum should returns the primary_keys and checksum of the found row' do
    begin
      session = ProxyConnection.new proxied_config.left

      session.insert_record('scanner_records', id: 1,
                                               name: 'Alice - exists in both databases')

      cursor = ProxyRowCursor.new session, 'scanner_records'
      cursor.prepare_fetch

      keys, checksum = cursor.next_row_keys_and_checksum

      expected_checksum = Digest::SHA1.hexdigest(
        Marshal.dump('id' => '1', 'name' => 'Alice - exists in both databases')
      )

      expect(keys).to eq({ 'id' => '1' })
      expect(checksum).to eq(expected_checksum)
    ensure
      session.execute('delete from scanner_records')
    end
  end
end
