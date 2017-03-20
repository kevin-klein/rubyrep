require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe ReplicationExtenders do
  before(:each) do
    Initializer.configuration = standard_config
    @@old_cache_status = ConnectionExtenders.use_db_connection_cache(false)
  end

  after(:each) do
    ConnectionExtenders.use_db_connection_cache(@@old_cache_status)
  end
  
  it "extenders should return list of registered connection extenders" do
    expect(ReplicationExtenders.extenders.include?(:postgresql)).to be_truthy
  end
  
  it "register should register a new connection extender" do
    ReplicationExtenders.register(:bla => :blub)
    
    expect(ReplicationExtenders.extenders.include?(:bla)).to be_truthy
  end
  
  it "register should replace already existing connection extenders" do
    ReplicationExtenders.register(:bla => :blub)
    ReplicationExtenders.register(:bla => :blub2)
    
    expect(ReplicationExtenders.extenders[:bla]).to eq(:blub2)
  end
end

