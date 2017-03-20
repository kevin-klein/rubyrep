class TRegistry

  def self.[](name)
    actor = Celluloid[name]
    while actor.dead?
      actor = Celluloid[name]
    end
    actor
  end
end
