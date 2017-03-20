require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TableSpecResolver do
  before(:each) do
    Initializer.configuration = standard_config
    @session = Session.new
    @resolver = TableSpecResolver.new @session
  end

  it "initialize should store the session and cache the tables of the session" do
    expect(@resolver.session).to eq(@session)
  end

  it "tables should return the tables of the specified database" do
    expect(@resolver.tables(:left)).to eq(@session.left.tables)
    expect(@resolver.tables(:right)).to eq(@session.right.tables)
  end
  
  it "resolve should resolve direct table names correctly" do
    expect(@resolver.resolve(['scanner_records', 'referenced_table'])).to eq([
      {:left => 'scanner_records', :right => 'scanner_records'},
      {:left => 'referenced_table', :right => 'referenced_table'}
    ])
  end
  
  it "resolve should resolve table name pairs correctly" do
    expect(@resolver.resolve(['left_table , right_table'])).to eq([
      {:left => 'left_table', :right => 'right_table'}
    ])
  end
  
  it "resolve should complain about non-existing tables" do
    expect {@resolver.resolve(['dummy, scanner_records'])}.
      to raise_error(/non-existing.*dummy/)
    expect {@resolver.resolve(['left_table, left_table'])}.
      to raise_error(/non-existing.*left_table/)
    expect {@resolver.resolve(['left_table'])}.
      to raise_error(/non-existing.*left_table/)
  end

  it "resolve should not complain about regexp specified tables not existing in right database" do
    expect(@resolver.resolve([/^scanner_records$/, /left_table/])).
      to eq([{:left => 'scanner_records', :right => 'scanner_records'}])
  end

  it "resolve should not check for non-existing tables if that is disabled" do
    expect {@resolver.resolve(['dummy, scanner_records'], [], false)}.
      not_to raise_error
  end

  it "resolve should resolve string in form of regular expression correctly" do
    expect(@resolver.resolve(['/SCANNER_RECORDS|scanner_text_key/']).sort { |a,b|
      a[:left] <=> b[:left]
    }).to eq([
      {:left => 'scanner_records', :right => 'scanner_records'},
      {:left => 'scanner_text_key', :right => 'scanner_text_key'}
    ])
  end

  it "resolve should resolve regular expressions correctly" do
    expect(@resolver.resolve([/SCANNER_RECORDS|scanner_text_key/]).sort { |a,b|
      a[:left] <=> b[:left]
    }).to eq([
      {:left => 'scanner_records', :right => 'scanner_records'},
      {:left => 'scanner_text_key', :right => 'scanner_text_key'}
    ])
  end

  it "resolve should should not return the same table multiple times" do
    expect(@resolver.resolve([
        'scanner_records',
        'scanner_records',
        'scanner_records, bla',
        '/scanner_records/'
      ]
    )).to eq([
      {:left => 'scanner_records', :right => 'scanner_records'}
    ])
  end

  it "resolve should not return tables that are excluded" do
    expect(@resolver.resolve(
      [/SCANNER_RECORDS|scanner_text_key/],
      [/scanner_text/]
    )).to eq([
      {:left => 'scanner_records', :right => 'scanner_records'},
    ])
  end

  it "non_existing_tables should return an empty hash if all tables exist" do
    table_pairs = [{:left => 'scanner_records', :right => 'referenced_table'}]
    expect(@resolver.non_existing_tables(table_pairs)).to eq({})
  end

  it "non_existing_tables should return a hash of non-existing tables" do
    table_pairs = [{:left => 'scanner_records', :right => 'bla'}]
    expect(@resolver.non_existing_tables(table_pairs)).to eq({:right => ['bla']})

    table_pairs = [
      {:left => 'blub', :right => 'bla'},
      {:left => 'scanner_records', :right => 'xyz'}
      ]
    expect(@resolver.non_existing_tables(table_pairs)).to eq({
      :left => ['blub'],
      :right => ['bla', 'xyz']
    })
  end

end