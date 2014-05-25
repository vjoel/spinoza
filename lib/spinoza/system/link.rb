require 'rbtree'
require 'spinoza/common'

# Models a comm link between nodes. Contains a channel data structure which is
# sorted on message arrival time.
class Spinoza::Link
  # Source and destination nodes.
  attr_reader :src, :dst
  
  # Delay between send by source and receive by destination.
  attr_reader :latency
  
  def initialize src: nil, dst: nil, latency: 0.100
    @src, @dst, @latency = src, dst, latency
    @channel = MultiRBTree.new
  end
  
  class << self
    alias [] new
  end
  
  # The src node calls this to send a message.
  def send msg
    if msg.nil?
      raise ArgumentError, "message cannot be nil"
    end
    @channel[dst.time_now + latency] = msg
  end
  
  # The dst node calls this to recv a message, if one is available.
  def recv
    t, m = @channel.first
    if t < dst.time_now
      @channel.shift
      m
    else
      nil
    end
  end
end
