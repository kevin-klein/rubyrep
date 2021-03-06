require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe NoisyConnection do
  before(:each) do
    Initializer.configuration = proxied_config
    @connection = ProxyConnection.new Initializer.configuration.left
    @connection.send(:extend, NoisyConnection)
    @connection.sweeper = TaskSweeper.new(1)
  end

  it "select_cursor should return correct results" do
    begin
      @connection.insert_record('scanner_records', {
        'id' => 1,
        'name' => 'Alice - exists in both databases'
      })

      expect(@connection.select_record(:table => 'scanner_records')).to eq({
        'id' => '1',
        'name' => 'Alice - exists in both databases'
      })
    ensure
      @connection.execute('delete from scanner_records')
    end
  end

  it "insert_record should write nil values correctly" do
    expect(@connection.sweeper).to receive(:ping).exactly(2).times
    begin
      @connection.insert_record('extender_combined_key', 'first_id' => 8, 'second_id' => '9', 'name' => nil)
      expect(@connection.select_one(
        "select name from extender_combined_key where (first_id, second_id) = (8, 9)"
      )).to eq({"name" => nil})
    ensure
      @connection.execute('delete from extender_combined_key')
    end
  end

  it "delete_record should delete the specified record" do
    expect(@connection.sweeper).to receive(:ping).exactly(2).times
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

  it "commit_db_transaction should update TaskSweeper" do
    initializer = ReplicationInitializer.new Session.new(standard_config)
    begin
      @connection.execute "insert into scanner_records(id,name) values(99, 'bla')"
      expect(@connection.select_one("select name from scanner_records where id = 99")['name']).
        to eq('bla')
    ensure
      @connection.execute "delete from scanner_records where id = 99"
    end
  end

end
