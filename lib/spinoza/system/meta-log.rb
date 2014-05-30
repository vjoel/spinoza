require 'spinoza/common'

# Model of synchronously replicated, linearizable global log, such as Zookeeper.
class Spinoza::MetaLog
  # Time to replicate a write to a quorum of MetaLog nodes, and for a unique
  # sequence number to be assigned, and for that to be communicated to the
  # writer node.
  attr_reader :dt_quorum
  
  # Delay for a write to become "completely" replicated: readable at all nodes.
  # Adjust this quantity for your network performance.
  attr_reader :dt_replicated

  class Entry
    # Node which wrote the entry.
    attr_reader :node
    
    # Data payload.
    attr_reader :value
    
    # When, in the global timeline, this entry has reached a quorum and the
    # writing node has been notified that this is the case.
    attr_reader :time_quorum

    # When, in the global timeline, this entry is completely replicated.
    attr_reader :time_replicated
    
    def initialize node: raise, value: raise,
        time_quorum: raise, time_replicated: raise
      @node, @value, @time_quorum, @time_replicated =
        node, value, time_quorum, time_replicated
    end
    
    # Returns true if the writing node knows the data is at a quorum.
    def quorum?
      @node.time_now >= @time_quorum
    end
    
    # Returns true if +other_node+ can read the entry (i.e. it has been
    # replicated to the nodes).
    def readable_at? other_node
      other_node == @node or other_node.time_now >= @time_replicated
    end
  end

  def initialize dt_quorum: 0.300, dt_replicated: 0.500
    @dt_quorum = dt_quorum
    @dt_replicated = dt_replicated
    @store = []
  end
  
  # Returns true if the writing node knows that the data at +id+ has been
  # replicated to a quorum of nodes.
  def quorum? id
    entry = @store[id]
    entry && entry.quorum?
  end
  
  def time_quorum id
    @store[id].time_quorum
  end

  def time_replicated id
    @store[id].time_replicated
  end

  # Append value to the MetaLog, assigning it a unique monotonically increasing
  # ID. In our use case, the value will be a key (or batch of keys) of the Log.
  # Returns an id, which can be used to retrieve the entry in the order it was
  # appended. The returned id should only be used to observe the model, and not
  # used within the model itself, since the id won't be available to the
  # requesting process until `time_quorum(id)`.
  def append value, node: raise
    @store << Entry.new(node: node, value: value,
      time_quorum: node.time_now + dt_quorum,
      time_replicated: node.time_now + dt_replicated)
    @store.size - 1
  end
  
  # Returns the value if the data has been propagated to +node+, otherwise,
  # returns nil.
  def get id, node: raise
    entry = @store[id]
    entry && entry.readable_at?(node) ? entry.value : nil
  end
end
