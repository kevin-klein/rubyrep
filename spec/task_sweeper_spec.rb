require File.dirname(__FILE__) + '/spec_helper.rb'

include RR

describe TaskSweeper do
  before(:each) do
  end

  it "should execute the given task" do
    x = nil
    TaskSweeper.timeout(1) {|sweeper| x = 1}
    expect(x).to eq(1)
  end

  it "should raise exceptions thrown by the task" do
    expect {
      TaskSweeper.timeout(1) {raise "bla"}
    }.to raise_error("bla")
  end

  it "should return if task stalls" do
    start = Time.now
    expect(TaskSweeper.timeout(0.01) {sleep 10}).to be_terminated
    expect(Time.now - start < 5).to be_truthy
  end

  it "should not return if task is active" do
    start = Time.now
    expect(TaskSweeper.timeout(0.1) do |sweeper|
      10.times do
        sleep 0.05
        sweeper.ping
      end
    end).not_to be_terminated
    expect(Time.now - start > 0.4).to be_truthy

  end

  it "should notify a stalled task about it's termination" do
    terminated = false
    TaskSweeper.timeout(0.01) do |sweeper|
      sleep 0.05
      terminated = sweeper.terminated?
    end.join
    expect(terminated).to be_truthy
  end
end