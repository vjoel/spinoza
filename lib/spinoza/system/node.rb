require 'spinoza/system/store'
require 'spinoza/system/table'
require 'spinoza/system/lock-manager'

# A top level entity in the system model, representing one node of
# the distributed system, typically one per host. Nodes are connected by Links.
class Spinoza::Node
  attr_reader :store
  attr_reader :lock_manager
  attr_reader :links
  
  # Local clock.
  attr_reader :time_now
  
  # Create a node whose store contains the specified tables and which has
  # its own lock manager.
  def initialize *tables
    @store = Store.new *tables
    @lock_manager = LockManager.new
    @links = []
    @time_now = 0.0
  end
  
  class << self
    alias [] new
  end
  
  def evolve dt
    @time_now += dt
  end
end
