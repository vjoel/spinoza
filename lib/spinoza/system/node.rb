require 'spinoza/system/model'
require 'spinoza/system/store'
require 'spinoza/system/table'
require 'spinoza/system/lock-manager'

# A top level entity in the system model, representing one node of
# the distributed system, typically one per host. Nodes are connected by Links.
# Node is stateful.
class Spinoza::Node < Spinoza::Model
  attr_reader :name
  attr_reader :store
  attr_reader :lock_manager
  
  # Outgoing links to peer nodes, as a map `{node => link, ...}`.
  # Use `links[node].send_message(msg)` to send a message to a peer.
  # Use Node#recv to handle received messages.
  attr_reader :links
  
  @next_name = 0
  def self.new_name
    @next_name.tap {@next_name += 1}.to_s
  end

  def new_name
    Spinoza::Node.new_name
  end
  
  # Create a node whose store contains the specified tables and which has
  # its own lock manager.
  def initialize *tables, name: new_name, **rest
    super **rest
    @store = Spinoza::Store.new *tables
    @name = name
    @lock_manager = Spinoza::LockManager.new
    @links = {}
  end
  
  def inspect
    "<Node #{name}>"
  end
  
  def to_s
    name.to_s
  end
  
  def link dst, **opts
    require 'spinoza/system/link'

    if links[dst]
      raise "Link from #{self} to #{dst} already exists."
    end
    links[dst] = Spinoza::Link[timeline: timeline, src: self, dst: dst, **opts]
  end

  def tables
    store.tables
  end
  
  class << self
    alias [] new
  end

  def recv msg: raise
    # Defined in subclasses.
  end
end
