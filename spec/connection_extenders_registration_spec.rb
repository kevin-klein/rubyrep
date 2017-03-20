require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ConnectionExtenders do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it "db_connect should install the already created logger" do
    configuration = deep_copy(Initializer.configuration)
    io = StringIO.new
    logger = ActiveSupport::Logger.new(io)
    configuration.left[:logger] = logger
    session = Session.new configuration

    expect(session.left.connection.instance_eval {@logger}).to eq(logger)
    expect(session.right.connection.instance_eval {@logger}).not_to eq(logger)

    session.left.select_one "select 'left_query'"
    session.right.select_one "select 'right_query'"

    expect(io.string).to match(/left_query/)
    expect(io.string).not_to match(/right_query/)
  end

  it "db_connect should create and install the specified logger" do
    configuration = deep_copy(Initializer.configuration)
    io = StringIO.new
    configuration.left[:logger] = io
    session = Session.new configuration
    session.left.select_one "select 'left_query'"
    session.right.select_one "select 'right_query'"

    expect(io.string).to match(/left_query/)
    expect(io.string).not_to match(/right_query/)
  end
end

describe ConnectionExtenders, "Registration" do
  before(:each) do
    Initializer.configuration = standard_config
    @old_cache_status = ConnectionExtenders.use_db_connection_cache(false)
  end

  after(:each) do
    ConnectionExtenders.use_db_connection_cache(@old_cache_status)
  end

  it "extenders should return list of registered connection extenders" do
    expect(ConnectionExtenders.extenders.include?(:postgresql)).to be_truthy
  end

  it "register should register a new connection extender" do
    ConnectionExtenders.register(:bla => :blub)

    expect(ConnectionExtenders.extenders.include?(:bla)).to be_truthy
  end

  it "register should replace already existing connection extenders" do
    ConnectionExtenders.register(:bla => :blub)
    ConnectionExtenders.register(:bla => :blub2)

    expect(ConnectionExtenders.extenders[:bla]).to eq(:blub2)
  end

  it "initialize should establish the database connections" do
    ConnectionExtenders.db_connect Initializer.configuration.left
  end

  it "db_connect created connections should be alive" do
    connection = ConnectionExtenders.db_connect Initializer.configuration.left

    expect(connection).to be_active
  end

  it "db_connect should include the connection extender into connection" do
    connection = ConnectionExtenders.db_connect Initializer.configuration.left

    expect(connection).to respond_to(:primary_key_names)
  end

  it "db_connect should raise an Exception if no fitting connection extender is available" do
    # If unknown connection adapters are encountered in jruby, then we
    # automatically use JdbcExtender.
    # Means that this test only makes sense if not running on jruby
    if not RUBY_PLATFORM =~ /java/

      config = deep_copy(Initializer.configuration)

      config.left[:adapter] = 'dummy'

      expect {ConnectionExtenders.db_connect  config.left}.to raise_error(RuntimeError, /dummy/)
    end
  end

  it "use_db_connection_cache should set the new cache status and return the old one" do
    ConnectionExtenders.use_db_connection_cache :first_status
    first_status = ConnectionExtenders.use_db_connection_cache :second_status
    second_status = ConnectionExtenders.use_db_connection_cache :whatever
    expect(first_status).to eq(:first_status)
    expect(second_status).to eq(:second_status)
  end

  it "clear_db_connection_cache should clear the connection cache" do
    old_cache = ConnectionExtenders.connection_cache
    begin
      ConnectionExtenders.connection_cache = :dummy_cache
      ConnectionExtenders.clear_db_connection_cache
      expect(ConnectionExtenders.connection_cache).to eq({})
    ensure
      ConnectionExtenders.connection_cache = old_cache
    end
  end

  it "db_connect should create the database connection if not yet cached" do
    old_cache = ConnectionExtenders.connection_cache
    begin
      ConnectionExtenders.clear_db_connection_cache
      ConnectionExtenders.use_db_connection_cache true
      ConnectionExtenders.db_connect Initializer.configuration.left
      expect(ConnectionExtenders.connection_cache).not_to be_empty
    ensure
      ConnectionExtenders.connection_cache = old_cache
    end
  end

  it "db_connect should not create the database connection if already cached and alive" do
    old_cache = ConnectionExtenders.connection_cache
    begin
      ConnectionExtenders.clear_db_connection_cache
      ConnectionExtenders.use_db_connection_cache true
      connection = ConnectionExtenders.db_connect Initializer.configuration.left
      expect(connection).to receive(:active?).and_return(:true)
      ConnectionExtenders.db_connect Initializer.configuration.left
    ensure
      ConnectionExtenders.connection_cache = old_cache
    end
  end

end
