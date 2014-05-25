require 'spinoza/common'

module Spinoza
  # Operations are stateless and do not reference any particular store. Hence
  # they can be serialized and passed around the system.
  class Operation
    # Name of table.
    attr_reader :table

    # Transaction this operation belongs to.

    # ds is Sequel dataset representing the table
    def execute ds
      raise
    end

    # check that this operation is allowed to execute
    # lm is LockManager
    def check lm
      raise
    end
  end

  class InsertOperation < Operation
    attr_reader :row
    def initialize txn = nil, table: table, row: row
      @txn = txn
      @table, @row = table, row
    end

    def execute ds
      ds << row
    end

    def check lm
      true
    end
  end

  class UpdateOperation < Operation
    attr_reader :key, :row
    def initialize txn = nil, table: table, row: row, key: key
      @txn = txn
      @table, @key, @row = table, key, row
    end

    def execute ds
      ds.where(key).update(row)
    end

    def check lm
      lm.has_write_lock? table, key, txn
    end
  end

  class DeleteOperation < Operation
    attr_reader :key
    def initialize txn = nil, table: table, key: key
      @txn = txn
      @table, @key = table, key
    end

    def execute ds
      ds.where(key).delete
    end
  end

  class ReadOperation < Operation
    attr_reader :key
    def initialize txn = nil, table: table, key: key
      @txn = txn
      @table, @key = table, key
    end

    def execute ds
      ReadResult.new(op: self, val: ds.where(key).all)
    end
  end

  # Result of executing a read operation on a particular node at a
  # particular time.
  class ReadResult
    attr_reader :op, :val
    def initialize op: op, val: val
      @op, @val = op, val
    end
  end
end