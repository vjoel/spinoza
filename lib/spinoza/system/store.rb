require 'sequel'
require 'spinoza/system/operation'

# Represents the storage at one node. In our model, this consists of an
# in-memory sqlite database with some very simple tables.
class Spinoza::Store
  def initialize *tables
    @db = Sequel.sqlite
    @tables = tables

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
  end
  
  def tables
    @db.tables
  end
  
  def inspect
    "<#{self.class.name}: #{tables.join(', ')}>"
  end
  
  # Execute the operations on this store, skipping any that do not refer to
  # a table in this store.
  def execute *operations
    results = operations.map do |op|
      if tables.include? op.table
        op.execute @db[op.table]
      end
    end
    results.grep(ReadResult)
  end
end
