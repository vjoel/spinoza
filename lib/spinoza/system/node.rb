require 'spinoza/system/store'
require 'spinoza/system/table-spec'
require 'spinoza/system/lock-manager'

# A top level entity in the system model, representing on whole node of
# the disrtibuted system, typically one per host.
class Spinoza::Node
  attr_reader :store
  attr_reader :lock_manager
  
  # Create a node whose store contains the specified tables and which has
  # its own lock manager.
  def initialize *table_specs
    @store = Store.new *table_specs
    @lock_manager = LockManager.new
  end
end
