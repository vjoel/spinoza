require 'spinoza/system/operation'
require 'set'

# A txn is just a list of operations, each on a specific table and key.
# The ACID guarantees are not provided by this class. Rather, this
# class is just common representation of a (very simple) kind of transaction
# that can be used by Calvin and other transaction engines built on top of
# the system model.
#
# Like the operations they are composed of, transactions are stateless and do
# not reference any particular store. Hence they can be re-used in different
# replicas.
module Spinoza; class Transaction
  attr_reader :ops

  class RowLocation
    def initialize txn, table, key
      @txn, @table, @key = txn, table, key
    end
    
    def insert row
      if @key and not @key.empty?
        raise ArgumentError, "Do not specify key in `at(...).insert(...)`."
      end
      @txn.ops << InsertOperation.new(@txn, table: @table, row: row)
    end

    def update row
      @txn.ops << UpdateOperation.new(@txn, table: @table, row: row, key: @key)
    end

    def delete
      @txn.ops << DeleteOperation.new(@txn, table: @table, key: @key)
    end

    def read
      @txn.ops << ReadOperation.new(@txn, table: @table, key: @key)
    end
  end

  # Build a transaction using a DSL. Example:
  #
  # txn = transaction do
  #   at(:persons, name: "Fred").update(age: 41, phrase: "yabba dabba doo")
  #   at(:persons, name: "Wilma").delete
  #   at(:persons, name: "Barney").read
  #   at(:persons).insert(name: "Betty", age: 65)
  # end
  #
  # node.store.execute txn # ==> [ReadResult(...)]
  #
  def initialize &block
    @ops = []
    if block
      if block.arity == 0
        instance_eval &block
      else
        yield self
      end
    end
  end
  
  def at table, **key
    RowLocation.new(self, table, key)
  end
  
  # {table => Set[key, ...]}
  def read_set
    scan_read_and_write_sets
    @read_set
  end
  
  # Set[table, table, ...]
  def write_set
    scan_read_and_write_sets
    @write_set
  end
  
  def scan_read_and_write_sets
    unless @read_set and @write_set
      @read_set = Hash.new {|h,k| h[k] = Set[]}
      @write_set = Set[]

      ops.each do |op|
        case op
        when ReadOperation
          @read_set[op.table] << op.key
        else
          @write_set << op.table
        end
      end
    end
  end

  def all_read_ops
    @all_read_ops ||= ops.grep(ReadOperation)
  end

  # returns true iff node contains elements of write set
  def active? node
    write_set.intersect? node.tables
  end
end; end
