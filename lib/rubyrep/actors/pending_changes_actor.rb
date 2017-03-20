class PendingChangesActor
  include TActor
  exclusive

  def initialize(session)
    @session = session
  end

  def has_changes?
    [:left, :right].any? do |database|
      next false if @session.configuration.send(database)[:mode] == :slave
      has_changes_in_db?(database)
    end
  end

  private

  def has_changes_in_db?(database)
    @session.send(database).select_one(
      "select id from #{@session.configuration.options[:rep_prefix]}_pending_changes limit 1"
    ) != nil
  end

end
