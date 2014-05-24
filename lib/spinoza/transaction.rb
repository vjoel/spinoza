require 'spinoza/system/operation'

class Spinoza::Transaction
  attr_reader :ops

  class RowLocation
    def initialize txn, table, key
      @txn, @table, @key = txn, table, key
    end
    
    def insert row
      @txn.ops << InsertOperation.new @txn, table: @table, row: row
    end

    def update row
      @txn.ops << UpdateOperation.new @txn, table: @table, row: row, key: @key
    end

    def delete
      @txn.ops << DeleteOperation.new @txn, table: @table, key: @key
    end

    def read
      @txn.ops << ReadOperation.new @txn, table: @table, key: @key
    end
  end

  # A txn is just a list of operations on a specific table and key. Example:
  #
  # txn = transaction do
  #   at("persons", name: "Fred").update(age: 41, phrase: "yabba dabba doo")
  #   at("persons", name: "Wilma").delete
  #   at("persons", name: "Barney").read
  #   at("persons", name: "Betty").insert(age: 65)
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
end
