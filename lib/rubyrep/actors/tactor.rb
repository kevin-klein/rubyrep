module TActor
  def self.included(klass)
    klass.class_eval do
      include Celluloid
    end
  end

  def cast(method, *args)
    begin
      future.send(method, *args)
      future.value
    rescue Celluloid::DeadActorError
      # Perhaps we got ahold of the actor before the supervisor restarted it
      retry
    end
  end
end
