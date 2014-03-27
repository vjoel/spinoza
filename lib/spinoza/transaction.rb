class Transaction

  # simple txn is just a list of operations on specific table and row id pairs.
  #
  # complex txn is SQL
  #
  def initialize
    @ops = []
  end
  
  def insert table, row
    @ops << [:insert, row]
  end

  def update table, row
    @ops << [:update, row]
  end

  def delete table, row_id
    @ops << [:delete, row_id]
  end

  def sql
    # mock the sequel api, recording methods to be applied later
  end
end
