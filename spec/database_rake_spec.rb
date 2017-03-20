require File.dirname(__FILE__) + '/spec_helper.rb'
load File.dirname(__FILE__) + '/../tasks/database.rake'

describe "database.rake" do
  before(:each) do
  end

  it "create_database should create a non-existing database" do
    expect(RR::ConnectionExtenders).to receive(:db_connect).and_raise("something")
    should_receive("`").with("PGPASSWORD= createdb \"dummy\" -h localhost -U  -E utf8")

    create_database :adapter => "postgresql", :database => "dummy"
  end

  it "create_database should not try to create existing databases" do
    expect(RR::ConnectionExtenders).to receive(:db_connect)
    should_receive(:puts).with("database existing_db already exists")

    create_database :adapter => 'postgresql', :database => "existing_db"
  end

  it "drop_database should drop a PostgreSQL database" do
    should_receive("`").with("PGPASSWORD= dropdb \"dummy\" -h localhost -U ")

    drop_database :adapter => "postgresql", :database => "dummy"
  end
end
