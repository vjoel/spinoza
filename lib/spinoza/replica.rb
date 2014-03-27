require 'sequel'

# No partitioning, just full replication of each table.
class Replica
  attr_reader :db
  
  def initialize
    @db = Sequel.sqlite
  end
  
  def has_papers!
    db.create_table? :papers do
      primary_key :id

      text    :name
      integer :lock # id of transaction locking this record, or nil
    end
  end

  def has_authors!
    db.create_table? :authors do
      primary_key :id

      text    :name
      integer :lock # id of transaction locking this record, or nil
    end
  end
  
  def has_authorship!
    db.create_table? :authorship do
      primary_key :id

      integer :paper_id, null: false
      integer :author_id, null: false
      integer :lock # id of transaction locking this record, or nil

      # No fkey constraints, since the referenced
      # table may not be present in this replica.
    end
  end
end

