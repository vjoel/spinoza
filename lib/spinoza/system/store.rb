require 'sequel'
require 'set'
require 'spinoza/system/operation'

# Represents the storage at one node. In our model, this consists of an
# in-memory sqlite database with some very simple tables. The state of the
# store is exactly the sqlite database.
class Spinoza::Store
  attr_reader :tables

  def initialize *tables
    @db = Sequel.sqlite

    tables.each do |table|
      @db.create_table table.name do
        table.columns.each do |col|
          case col.type
          when "integer", "string", "float"
            column col.name, col.type, primary_key: col.primary
          else
            raise ArgumentError,
              "Bad col.type: #{col.type} in table #{table.name}"
          end
        end
      end
    end

    @tables = Set[*@db.tables]
  end
  
  def inspect
    "<#{self.class.name}: #{tables.to_a.join(', ')}>"
  end
  
  # Execute the operations on this store, skipping any that do not refer to
  # a table in this store. Returns array of all read results.
  def execute *operations
    results = operations.map do |op|
      if tables.include? op.table
        op.execute @db[op.table]
      end
    end
    results.grep(Spinoza::ReadResult)
  end
end
