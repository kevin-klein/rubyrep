require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe Syncers do
  before(:each) do
    @old_syncers = Syncers.syncers
  end

  after(:each) do
    Syncers.instance_variable_set :@syncers, @old_syncers
  end

  it 'syncers should return empty hash if nil' do
    Syncers.instance_variable_set :@syncers, nil
    expect(Syncers.syncers).to eq({})
  end

  it 'syncers should return the registered syncers' do
    Syncers.instance_variable_set :@syncers, :dummy_data
    expect(Syncers.syncers).to eq(:dummy_data)
  end

  it 'configured_syncer should return the correct syncer as per :syncer option, if both :syncer and :replicator is configured' do
    options = {
      syncer: :two_way,
      replicator: :key2
    }
    expect(Syncers.configured_syncer(options)).to eq(Syncers::TwoWaySyncer)
  end

  it 'configured_syncer should return the correct syncer as per :replicator option if no :syncer option is provided' do
    options = { replicator: :two_way }
    expect(Syncers.configured_syncer(options)).to eq(Syncers::TwoWaySyncer)
  end

  it 'register should register the provided commiter' do
    Syncers.instance_variable_set :@syncers, nil
    Syncers.register a_key: :a
    Syncers.register b_key: :b
    expect(Syncers.syncers[:a_key]).to eq(:a)
    expect(Syncers.syncers[:b_key]).to eq(:b)
  end
end

describe Syncers::OneWaySyncer do
  before(:each) do
    Initializer.configuration = standard_config
  end

  it 'should register itself' do
    expect(Syncers.syncers[:one_way]).to eq(Syncers::OneWaySyncer)
  end

  it 'initialize should store sync_helper' do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    syncer = Syncers::OneWaySyncer.new(helper)
    expect(syncer.sync_helper).to eq(helper)
  end

  it 'initialize should calculate course source, target and source_record_index' do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)

    # verify correct behaviour if syncing to the left
    allow(helper).to receive(:sync_options).and_return(direction: :left, sync_conflict_handling: :right_wins, logged_sync_events: :all_changes, right_record_handling: :insert, left_record_handling: :insert)
    syncer = Syncers::OneWaySyncer.new(helper)
  end

  it 'default_option should return the correct default options' do
    expect(Syncers::OneWaySyncer.default_options).to eq({ left_record_handling: :insert, right_record_handling: :insert, sync_conflict_handling: :ignore, logged_sync_events: [:ignored_conflicts] })
  end

  it 'sync_difference should delete in the right database' do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    allow(helper).to receive(:sync_options).and_return(rep_prefix: 'rr', direction: :left, sync_conflict_handling: :right_wins, logged_sync_events: :all_changes, right_record_handling: :insert, left_record_handling: :insert)
    syncer = Syncers::OneWaySyncer.new(helper)
    expect(helper).to receive(:delete_record).with(:left, 'scanner_records', :dummy_record)
    expect(helper).not_to receive(:update_record)
    syncer.sync_difference(:left, :dummy_record)

    helper = SyncHelper.new(sync)
    allow(helper).to receive(:sync_options).and_return(direction: :right, delete: true)
    syncer = Syncers::OneWaySyncer.new(helper)
    expect(helper).to receive(:delete_record).with(:right, 'scanner_records', :dummy_record)
    syncer.sync_difference(:right, :dummy_record)
  end

  it 'sync_difference should not insert if :insert option is not true' do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    allow(helper).to receive(:sync_options).and_return(direction: :left, insert: false)
    syncer = Syncers::OneWaySyncer.new(helper)
    expect(helper).not_to receive(:delete_record)
    expect(helper).not_to receive(:update_record)
    expect(helper).not_to receive(:insert_record)
    syncer.sync_difference(:right, :dummy_record)
  end

  it 'sync_difference should insert in the right database' do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    allow(helper).to receive(:sync_options).and_return(direction: :left, insert: true)
    syncer = Syncers::OneWaySyncer.new(helper)
    expect(helper).not_to receive(:delete_record)
    expect(helper).not_to receive(:update_record)
    expect(helper).to receive(:insert_record).with(:left, 'scanner_records', :dummy_record)
    syncer.sync_difference(:right, :dummy_record)

    helper = SyncHelper.new(sync)
    allow(helper).to receive(:sync_options).and_return(direction: :right, insert: true)
    syncer = Syncers::OneWaySyncer.new(helper)
    expect(helper).to receive(:insert_record).with(:right, 'scanner_records', :dummy_record)
    syncer.sync_difference(:left, :dummy_record)
  end

  it 'sync_difference should not update if :update option is not true' do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    allow(helper).to receive(:sync_options).and_return(direction: :left, update: false)
    syncer = Syncers::OneWaySyncer.new(helper)
    expect(helper).not_to receive(:delete_record)
    expect(helper).not_to receive(:update_record)
    expect(helper).not_to receive(:insert_record)
    syncer.sync_difference(:conflict, :dummy_records)
  end

  it 'sync_difference should update the right values in the right database' do
    sync = TableSync.new(Session.new, 'scanner_records')
    helper = SyncHelper.new(sync)
    allow(helper).to receive(:sync_options).and_return(direction: :left, update: true)
    syncer = Syncers::OneWaySyncer.new(helper)
    expect(helper).not_to receive(:delete_record)
    expect(helper).to receive(:update_record).with(:left, 'scanner_records', :right_record)
    expect(helper).not_to receive(:insert_record)
    syncer.sync_difference(:conflict, [:left_record, :right_record])

    helper = SyncHelper.new(sync)
    allow(helper).to receive(:sync_options).and_return(direction: :right, update: true)
    syncer = Syncers::OneWaySyncer.new(helper)
    expect(helper).to receive(:update_record).with(:right, 'scanner_records', :left_record)
    syncer.sync_difference(:conflict, [:left_record, :right_record])
  end
end
