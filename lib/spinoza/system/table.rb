require 'spinoza/common'

# Defines the schema for one table in all replicas. No state.
class Spinoza::Table
  attr_reader :name
  attr_reader :columns
  
  class Column
    attr_reader :name
    attr_reader :type
    attr_reader :primary
    
    def initialize name, type, primary
      @name, @type, @primary = name, type, primary
    end
  end
  
  # Usage:
  #
  #   create_table :Widgets, id: "integer", weight: "float", name: "string"
  #
  # The first column is the primary key for the table.
  #
  def initialize name, **specs
    @name = name
    @columns = []
    specs.each_with_index do |(col_name, col_type), i|
      @columns << Column.new(col_name, col_type, (i==0))
    end
  end
  
  class << self
    alias [] new
  end
end
