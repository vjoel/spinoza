require 'spinoza/common'

# Manages concurrency in the spinoza system model, which explicitly schedules
# all database reads and writes. So all this does is check for concurrency
# violations; nothing actually blocks.
#
# The +txn+ references in this class care only about identity, so they could
# all be ids or they could all be transaction objects. Similarly the resource
# being locked by a ReadLock or WriteLock can be anything whose identity is
# defined by hash equality, i.e. #eql?. Typically, we use `[table, key]` pairs,
# where `key` is a primary key reference like `{id: ...}`.
#
class Spinoza::LockManager
  class ConcurrencyError < StandardError; end
  
  # State of lock on one resource (e.g., a row), which may be held concurrently
  # by any number of threads. Reentrant.
  class ReadLock
    attr_reader :txns

    def initialize txn
      @txns = Hash.new(0)
      add txn
    end
    
    def unlocked?
      @txns.all? {|t,c| c == 0}
    end
    
    def includes? txn
      @txns[txn] > 0
    end
    
    def add txn
      @txns[txn] += 1
    end
    
    def remove txn
      if includes? txn
        @txns[txn] -= 1
        @txns.delete txn if @txns[txn] == 0
      else
        raise ConcurrencyError, "#{self} is not locked by #{txn}"
      end
    end
  end

  # State of lock on one resource (e.g., a row), which may be held concurrently
  # by at most one thread. Reentrant.
  class WriteLock
    attr_reader :txn, :count

    def initialize txn
      @txn = txn
      @count = 1
    end
    
    def unlocked?
      @txn == nil
    end

    def includes? txn
      @txn == txn && @count > 0
    end
    
    def add txn
      if includes? txn
        @count += 1
      else
        raise ConcurrencyError, "#{self} is not locked by #{txn}"
      end
    end

    def remove txn
      if includes? txn
        @count -= 1
        @txn = nil if @count == 0
      else
        raise ConcurrencyError, "#{self} is not locked by #{txn}"
      end
    end
  end

  # { resource => WriteLock | ReadLock | nil, ... }
  # typically, resource == [table, key]
  attr_reader :locks
  
  def initialize
    @locks = {}
  end
  
  def lock_read resource, txn
    case lock = locks[resource]
    when nil
      locks[resource] = ReadLock.new(txn)
    when ReadLock, WriteLock
      lock.add txn
        # in WriteLock case, add the reader as a writer
        # (fails if not locked by txn)
    else raise
    end
  end
  
  def lock_write resource, txn
    case lock = locks[resource]
    when nil
      locks[resource] = WriteLock.new(txn)
    when WriteLock
      lock.add txn
    when ReadLock
      raise ConcurrencyError, "#{resource} is locked: #{lock}"
    else raise
    end
  end
  
  def unlock_read resource, txn
    lock = locks[resource]
    case lock
    when nil
      raise ConcurrencyError, "#{resource} is not locked"
    when ReadLock, WriteLock
      begin
        lock.remove txn
        locks.delete resource if lock.unlocked?
      rescue ConcurrencyError => ex
        raise ConcurrencyError "#{resource}: #{ex.message}"
      end
    else raise
    end
  end
  
  def unlock_write resource, txn
    lock = locks[resource]
    case lock
    when ReadLock
      raise ConcurrencyError, "#{resource} is read locked: #{lock}"
    when nil
      raise ConcurrencyError, "#{resource} is not locked"
    when WriteLock
      begin
        lock.remove txn
        locks.delete resource if lock.unlocked?
      rescue ConcurrencyError => ex
        raise ConcurrencyError "#{resource}: #{ex.message}"
      end
    else raise
    end
  end
  
  def unlock_all txn
    locks.delete_if do |resource, lock|
      if lock and lock.includes? txn
        lock.remove txn
        lock.unlocked?
      end
    end
  end
  
  def has_read_lock? resource, txn
    lock = locks[resource]
    lock.kind_of?(ReadLock) && lock.includes?(txn)
  end
  
  def has_write_lock? resource, txn
    lock = locks[resource]
    lock.kind_of?(WriteLock) && lock.includes?(txn)
  end
end
