require 'spinoza/system/model'
require 'spinoza/system/timeline'

# Models a comm link between nodes, including the latency between sender and
# receiver. The class is stateless: the state of the channnel (messages and
# their scheduled arrivals) is part of the global timeline.
class Spinoza::Link < Spinoza::Model
  # Source and destination nodes.
  attr_reader :src, :dst
  
  # Delay between send by source and receive by destination.
  attr_reader :latency
  
  def initialize src: nil, dst: nil, latency: 0.100, **rest
    super **rest
    @src, @dst, @latency = src, dst, latency
  end
  
  class << self
    alias [] new
  end
  
  # The src node calls this to send a message. The message is scheduled for
  # arrival at the destination.
  def send msg
    timeline << Event[node: dst, time: time_now + latency,
      action: :recv, msg: msg]
  end
end
