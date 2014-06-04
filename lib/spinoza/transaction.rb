require 'spinoza/system/operation'
require 'set'

# A transaction is just a list of operations, each on a specific table and key.
#
# The ACID guarantees are not provided by this class. Rather, this
# class is just common representation of a (very simple) kind of transaction
# that can be used by Calvin and other transaction engines built on top of
# the system model.
#
# Transactions submitted to Calvin are not as general as this class indicates.
# They should be split up into transactions each of which consists of reads
# followed by writes, and the row IDs of the writes should be independent of the
# read results. This is necessary for the Scheduler and the Readcaster
# protocols. (See the discussion of OLLP in the Calvin papers.)
#
# Like the operations they are composed of, transactions are stateless and do
# not reference any particular store. Hence they can be re-used in different
# replicas.
#
module Spinoza
  class Transaction
    class RowLocation
      def initialize txn, table, key
        @txn, @table, @key = txn, table, key
      end

      def insert row
        if @key and not @key.empty?
          raise ArgumentError, "Do not specify key in `at(...).insert(...)`."
        end
        @txn << InsertOperation.new(@txn, table: @table, row: row)
      end

      def update row
        @txn << UpdateOperation.new(@txn, table: @table, row: row, key: @key)
      end

      def delete
        @txn << DeleteOperation.new(@txn, table: @table, key: @key)
      end

      def read
        @txn << ReadOperation.new(@txn, table: @table, key: @key)
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
    # If the block takes an argument, then the Transaction instance is passed,
    # and the block is not instance_eval-ed.
    #
    # If you create a transaction without using the block interface, you
    # must call #closed! before using the transaction.
    #
    def initialize &block
      @ops = []
      @closed = false
      @read_set = Hash.new {|h,k| h[k] = Set[]}
      @write_set = Hash.new {|h,k| h[k] = Set[]}

      if block
        if block.arity == 0
          instance_eval &block
        else
          yield self
        end
        closed!
      end
    end

    class StateError < StandardError; end

    def closed?
      @closed
    end

    def closed!
      assert_open!
      @closed = true
    end

    def assert_closed!
      unless closed?
        raise StateError, "transaction must be closed to call that method."
      end
    end

    def assert_open!
      if closed?
        raise StateError, "transaction must be open to call that method."
      end
    end

    def at table, **key
      RowLocation.new(self, table, key)
    end

    INSERT_KEY = :insert

    def << op
      assert_open!

      case op
      when ReadOperation
        if reads_a_write?(op)
          warn "reading your writes in a transaction may not be supported #{op}"
        end
        @read_set[op.table] << op.key
      when InsertOperation
        @write_set[op.table] << INSERT_KEY
      else
        @write_set[op.table] << op.key
      end
      @ops << op
    end

    def reads_a_write? op
      @write_set[op.table].include? op.key
    end

    # {table => Set[key, ...]}
    def read_set
      assert_closed!
      @read_set
    end

    # Set[table, table, ...]
    def read_tables
      @read_tables ||= Set[*read_set.keys]
    end

    # {table => Set[key|INSERT_KEY, ...]}
    # where existence of INSERT_KEY means presence of inserts in txn
    def write_set
      assert_closed!
      @write_set
    end

    # Set[table, table, ...]
    def write_tables
      @write_tables ||= Set[*write_set.keys]
    end

    def all_read_ops
      assert_closed!
      @all_read_ops ||= @ops.grep(ReadOperation)
    end

    def all_write_ops
      assert_closed!
      @all_write_ops ||= @ops - all_read_ops
    end

    def ops
      assert_closed!
      @ops
    end

    # returns true iff node_or_store contains elements of write set
    def active? node_or_store
      write_tables.intersect? node_or_store.tables
    end

    def all_reads_are_local? node_or_store
      read_tables.subset? node_or_store.tables
    end

    def remote_read_tables node_or_store
      read_tables - node_or_store.tables
    end
  end
end
