require 'spinoza/common'

# Manages concurrency in the spinoza system model, which explicitly schedules
# all database reads and writes.
class Spinoza::LockManager
  class ConcurrencyError < StandardError; end
  
  class ReadLock
    attr_reader :txns

    def initialize txn
      @txns = Hash.new(0)
      add txn
    end
    
    def unlocked
      @txns.empty?
    end
    
    def includes? txn
      @txns[txn] && @txns[txn] > 0
    end
    
    def add txn
      @txns[txn] += 1
    end
    
    def remove txn
      if @txns[txn] > 0
        count = (@txns[txn] -= 1)
        if count == 0
          @txns.delete txn
        end
      else
        raise ConcurrencyError, "not locked by this transaction: #{self}"
      end
    end
  end

  class WriteLock
    attr_reader :txn

    def initialize txn
      @txn = txn
    end
    
    def includes? txn
      @txn == txn
    end

    def remove txn
      if @txn == txn
        @txn = nil
      else
        raise ConcurrencyError, "not locked by this transaction: #{self}"
      end
    end
  end

   # { [table, key] => WriteLock | ReadLock | nil, ... }
  attr_reader :locks
  
  def initialize
    @locks = {}
  end
  
  def lock_read table, key, txn
    case lock = locks[ [table, key] ]
    when nil
      locks[ [table, key] ] = ReadLock.new(txn)
    when ReadLock
      lock.add txn
    when WriteLock
      raise ConcurrencyError, "#{[table, key]} is locked: #{lock}"
    end
  end
  
  def lock_write table, key, txn
    case lock = locks[ [table, key] ]
    when nil
      locks[ [table, key] ] = WriteLock.new(txn)
    else
      raise ConcurrencyError, "#{[table, key]} is locked: #{lock}"
    end
  end
  
  def unlock_read table, key, txn
    lock = locks[ [table, key] ]
    case lock
    when WriteLock
      raise ConcurrencyError, "#{[table, key]} is write locked: #{lock}"
    when nil
      raise ConcurrencyError, "#{[table, key]} is not locked"
    else
      begin
        lock.remove txn
        if lock.unlocked
          locks.delete [table, key]
        end
      rescue ConcurrencyError => ex
        raise ConcurrencyError "#{[table, key]} is #{ex.message}"
      end
    end
  end
  
  def unlock_write table, key, txn
    lock = locks[ [table, key] ]
    case lock
    when ReadLock
      raise ConcurrencyError, "#{[table, key]} is read locked: #{lock}"
    when nil
      raise ConcurrencyError, "#{[table, key]} is not locked"
    else
      begin
        lock.remove txn
        locks.delete [table, key]
      rescue ConcurrencyError => ex
        raise ConcurrencyError "#{[table, key]} is #{ex.message}"
      end
    end
  end
  
  def has_read_lock? table, key, txn
    lock = locks[ [table, key] ]
    lock.kind_of?(ReadLock) && lock.includes?(txn)
  end
  
  def has_write_lock? table, key, txn
    lock = locks[ [table, key] ]
    lock.kind_of?(WriteLock) && lock.includes?(txn)
  end
  
  ### block versions (yield)?
end
