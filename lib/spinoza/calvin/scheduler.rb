require 'spinoza/common'
require 'spinoza/calvin/executor'
require 'spinoza/calvin/readcaster'

class Calvin::Scheduler
  attr_reader :node

  attr_reader :executors
  attr_reader :idle_executors

  # Maps { locally executing transaction => Executor }
  attr_reader :ex_for_txn
  
  # Transactions to be executed, in order.
  attr_reader :work_queue

  def initialize node: raise, n_threads: 4
    @node = node

    @executors = n_threads.times.map {
      Calvin::Executor.new(
        store: node.store,
        readcaster: Calvin::Readcaster.new(node: node))}

    @idle_executors = @executors.dup
    @ex_for_txn = {}
    @work_queue = []
    
    node.meta_log.on_entry_available self, :handle_meta_log_entry
  end

  def handle_meta_log_entry id: raise, node: raise, value: raise
    batch_id = value
    batch = node.read_batch(batch_id)
    if batch
      work_queue.concat batch
      handle_next_transactions
    else
      # Log entry did not yet propagate to this node, even though MetaLog entry
      # did propagate. Won't happen with default latency settings.
      raise "TODO" ##
    end
  end

  # Handle messages from peers. The only messages are the unidirectional
  # broadcasts of read results.
  def recv transaction: raise, table: raise, read_results: raise
    ex = ex_for_txn[transaction]
    if ex
      result = ex.recv_remote_reads table, read_results
      if result
        finish_transaction transaction, result
        handle_next_transactions
      end
    else
      ## TODO what if transaction hasn't started yet? Buffer? This won't
      ## happen with our simplistic latency assumptions.
      # The transaction has already finished locally, but another
      # node is still sending out read results.
    end
  end

  def handle_next_transactions
    until work_queue.empty? or idle_executors.empty?
      success = handle_next_transaction
      break unless success
    end
  end
  
  def handle_next_transaction
    ex = idle_executors.last
    txn = work_queue.first
    raise if ex_for_txn[txn]

    lock_succeeded = try_lock(txn)
    
    if lock_succeeded
      txn = work_queue.shift
      result = ex.execute_transaction(txn)
      if result
        finish_transaction txn, result
      else
        idle_executors.pop
        ex_for_txn[txn] = ex
      end

    else
      node.lock_manager.unlock_all transaction
      # nothing to do until some executor finishes its current transaction
      ## TODO optimization: attempt to reorder another txn to the head
      ## of the work_queue where lock sets are disjoint.
    end

    lock_succeeded
  end

  def try_lock txn
    lm = node.lock_manager
    rset = txn.read_set
    wset = txn.write_set

    # get write locks first, so r/w on same key doesn't fail
    wset.each do |table, keys|
      keys.each do |key|
        next if key == Transaction::INSERT_KEY
        lock_write [table, key], txn
      end
    end

    rset.each do |table, keys|
      keys.each do |key|
        lock_read [table, key], txn
      end
    end

    true

  rescue Spinoza::LockManager::ConcurrencyError
    false
  end

  def finish_transaction transaction, result
    ex = ex_for_txn.delete(transaction)
    idle_executors.push ex
    node.lock_manager.unlock_all transaction
    node.finished_transaction transaction, result
  end
end
