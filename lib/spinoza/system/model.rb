require 'spinoza/common'

# Base class for all model classes that know about the passage of time.
class Spinoza::Model
  attr_reader :timeline

  def initialize timeline: nil
    @timeline = timeline
  end

  def time_now
    timeline.now
  end
end
