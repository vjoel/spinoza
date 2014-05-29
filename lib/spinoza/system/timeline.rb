require 'rbtree'
require 'spinoza/common'

class Spinoza::Event
  attr_reader :time, :actor, :action, :data

  class << self; alias [] new; end

  def initialize time: raise, actor: raise, action: raise, **data
    @time, @actor, @action, @data = time, actor, action, data
  end
  
  def dispatch
    actor.send action, **data
  end
end

# A timeline of events, future and past, and a current time.
class Spinoza::Timeline
  class Error < StandardError; end
  
  attr_reader :now, :future, :history
  
  def initialize
    @history = MultiRBTree.new
    @future = MultiRBTree.new
    @now = 0.0
  end
  
  def schedule event
    @future[event.time] = event
  end
  alias << schedule
  
  # Dispatches the next event. If several events are scheduled at the same time,
  # dispatches the one that was scheduled first. Returns nil if nothing is
  # scheduled, otherwise returns current time, which is when the event was
  # dispatched. Dispatched events are stored in a history timeline. Does not
  # advance time if there is no event scheduled.
  def step
    time, event = @future.shift
    return nil unless time
    dispatch_event event
    return now
  end
  
  # Dispatches all events in sequence for the next +dt+ units of time, in other
  # words, all events scheduled in the interval `now..now+dt`. If several events
  # are scheduled at the same time, dispatches the one that was scheduled first.
  # Dispatched events are stored in a history timeline. Returns the time at the
  # end of the interval, which may be later than the time of any dispatched
  # event. Advances time even if no events exist in the interval.
  def evolve dt
    t_end = now + dt
    loop do
      time, event = @future.first
      break if not time or time > t_end
      @future.shift
      dispatch_event event
      yield event if block_given?
    end
    @now = t_end
  end

  private def dispatch_event event
    if event.time < now
      raise Timeline::Error, "Event scheduled in the past: #{event}"
    end

    @now = event.time
    @history[event.time] = event
    event.dispatch
  end
end
