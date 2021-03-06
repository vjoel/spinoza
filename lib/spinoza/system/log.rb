require 'spinoza/common'

# Model of asynchronously replicated global log, such as Cassandra. We assume
# that each node in our system has a replica providing this service.
class Spinoza::Log
  # Delay for a write to become durable on "enough" replicas, from the point of
  # view of the writing node. Adjust this quantity for your definition of
  # durable and your network performance.
  attr_reader :dt_durable

  # Delay for a write to become "completely" replicated: readable at all nodes.
  # Adjust this quantity for your network performance.
  attr_reader :dt_replicated
  
  # We do not allow the same key to be written twice, since a key uniquely
  # designates the logged transaction request. This error is raised if a
  # key is overwritten.
  class KeyConflictError < StandardError; end

  class Entry
    # Node which wrote the entry.
    attr_reader :node
    
    # Data payload.
    attr_reader :value
    
    # When, in the global timeline, this entry is durable enough and the
    # writing node has been notified that this is the case.
    attr_reader :time_durable

    # When, in the global timeline, this entry is completely replicated.
    attr_reader :time_replicated
    
    def initialize node: raise, value: raise,
        time_durable: raise, time_replicated: raise
      @node, @value, @time_durable, @time_replicated =
        node, value, time_durable, time_replicated
    end
    
    # Returns true if the writing node believes the data to be durable.
    def durable?
      @node.time_now >= @time_durable
    end
    
    # Returns true if +other_node+ can read the entry (i.e. it has been
    # replicated to the nodes).
    def readable_at? other_node
      other_node == @node or other_node.time_now >= @time_replicated
    end
  end

  def initialize dt_durable: 0.300, dt_replicated: 0.500
    @dt_durable = dt_durable
    @dt_replicated = dt_replicated
    @store = {}
  end
  
  # Returns true if the writing node believes the data at +key+ is durable.
  def durable? key
    entry = @store[key]
    entry && entry.durable?
  end
  
  def time_durable key
    @store[key].time_durable
  end

  def when_durable key, **event_opts
    entry = @store[key]
    entry.node.timeline.schedule Spinoza::Event[
      time: entry.time_durable,
      **event_opts
    ]
  end

  def time_replicated key
    @store[key].time_replicated
  end

  # Returns the entry.
  def write key, value, node: raise
    raise KeyConflictError if @store[key] or not value
    @store[key] =
      Entry.new node: node, value: value,
        time_durable: node.time_now + dt_durable,
        time_replicated: node.time_now + dt_replicated
  end
  
  # Returns the value if the data has been propagated to +node+, otherwise,
  # returns nil.
  def read key, node: raise
    entry = @store[key]
    entry && entry.readable_at?(node) ? entry.value : nil
  end
end
