require 'spinoza/common'

# Represents the work performed in one thread. The scheduler assigns a sequence
# of transactions to each of several executors. The executor handles the
# transactions one at a time, in a series of substeps as data is received from
# peers. Within an executor, the sequence of transactions and substeps is
# totally ordered wrt the global timeline, but the sequences of two Executors
# may interleave, which is how Calvin achieves some write concurrency.
#
# Does not have access to any subsystems except the node's Store and
# communication with peer executors via the readcasters.
#
class Calvin::Executor
  attr_reader :store
  attr_reader :readcaster
  attr_reader :task
  
  class StateError < StandardError; end
  
  # Represents the state of executing one transaction in this Executor, in
  # the case where that execution involves waiting for data from peers.
  class Task
    attr_reader :txn
    
    # Accumulates results as they arrive locally and from peers.
    attr_accessor :read_results
    
    # Set of tables the task is waiting for.
    attr_accessor :remote_read_tables

    def initialize txn, read_results: [], remote_read_tables: Set[]
      @txn = txn
      @read_results = read_results
      @remote_read_tables = remote_read_tables
    end
  end
  
  def initialize store: nil, readcaster: nil
    @store = store
    @readcaster = readcaster
    ready!
  end
  
  def ready!
    @task = nil
  end
  
  def ready?
    @task.nil?
  end
  
  def assert_ready?
    unless ready?
      raise StateError, "cannot start new task -- already executing #{task}"
    end
  end

  # Assumes all locks are held around this call.
  def execute_transaction txn
    assert_ready?

    local_read_results = @readcaster.execute_local_reads txn
    @readcaster.serve_reads txn, local_read_results

    if passive? txn
      result = local_read_results
      ready!

    elsif all_reads_are_local? txn
      result = local_read_results
      store.execute *txn.all_write_ops
      ready!

    else
      @task = Task.new txn,
        read_results: local_read_results,
        remote_read_tables: txn.remote_read_tables(store)
      result = false
    end

    return result
  end
  
  def passive? txn
    not txn.active? store
  end

  def all_reads_are_local? txn
    txn.all_reads_are_local? store
  end
    
  # Assumes all locks are held around this call.
  def recv_remote_reads table, read_results
    if task.remote_read_tables.include? table
      task.remote_read_tables.delete table
      task.read_results.concat read_results
    # else this is a redundant message for this table, so ignore it
    end

    return false unless task.remote_read_tables.empty?

    store.execute *task.txn.all_write_ops
    result = task.read_results
    ready!
    result
  end
end
