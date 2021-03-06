require 'minitest/autorun'
require 'spinoza/system/node'

class TestNode < Minitest::Test
  include Spinoza

  def setup
    @node = Node[
      Table[:foos, id: "integer", name: "string", len: "float"],
      timeline: nil
    ]
  end
  
  def test_store
    assert_equal Set[:foos], @node.store.tables
  end
  
  def test_ops
    store = @node.store
    
    op_i = InsertOperation.new(table: :foos, row: {id: 1, name: "a", len: 1.2})
    op_r = ReadOperation.new(table: :foos, key: {id: 1})
    rslt = store.execute op_i, op_r
    
    assert_equal(1, rslt.size)
    assert_equal({id: 1, name: "a", len: 1.2}, rslt[0].val)
    
    op_u = UpdateOperation.new(table: :foos, key: {id: 1}, row: {name: "b"})
    rslt = store.execute op_u, op_r
    
    assert_equal(1, rslt.size)
    assert_equal({id: 1, name: "b", len: 1.2}, rslt[0].val)

    op_d = DeleteOperation.new(table: :foos, key: {id: 1})
    rslt = store.execute op_d, op_r
    assert_equal(1, rslt.size)
    refute rslt[0].val
  end
  
  def test_locks
    lm = @node.lock_manager
    
    rs1 = [:foos, 1]
    rs2 = [:foos, 2]
    
    lm.lock_read rs1, :t1
    assert lm.has_read_lock?(rs1, :t1)
    refute lm.has_read_lock?(rs1, :t2)
    refute lm.has_read_lock?([:bars, 3], :t1)

    lm.lock_read rs1, :t2
    assert lm.has_read_lock?(rs1, :t1)
    assert lm.has_read_lock?(rs1, :t2)

    lm.unlock_read rs1, :t1
    refute lm.has_read_lock?(rs1, :t1)
    assert lm.has_read_lock?(rs1, :t2)
    
    assert_raises LockManager::ConcurrencyError do
      lm.lock_write rs1, :t1
    end

    lm.lock_read rs1, :t2
    assert lm.has_read_lock?(rs1, :t2)
    lm.unlock_read rs1, :t2
    assert lm.has_read_lock?(rs1, :t2)
    lm.unlock_read rs1, :t2
    refute lm.has_read_lock?(rs1, :t2)
    
    lm.lock_write rs2, :t1
    assert lm.has_write_lock?(rs2, :t1)
    assert_raises LockManager::ConcurrencyError do
      lm.lock_read rs2, :t2
    end
    assert lm.has_write_lock?(rs2, :t1)
    refute lm.has_write_lock?(rs2, :t3)
    refute lm.has_write_lock?([:bars, 3], :t1)

    lm.lock_write rs2, :t1
    assert lm.has_write_lock?(rs2, :t1)
    lm.unlock_write rs2, :t1
    assert lm.has_write_lock?(rs2, :t1)
    lm.unlock_write rs2, :t1
    refute lm.has_write_lock?(rs2, :t1)
  end
    
  def test_unlock_all
    lm = @node.lock_manager
    
    rs1 = [:foos, 1]
    rs2 = [:foos, 2]

    lm.lock_read rs1, :t1
    lm.lock_write rs2, :t2

    lm.unlock_all :t1
    refute lm.has_write_lock?(rs1, :t1)
    refute lm.has_write_lock?(rs2, :t1)
    refute lm.has_write_lock?(rs1, :t2)
    assert lm.has_write_lock?(rs2, :t2)

    lm.unlock_all :t2
    refute lm.has_write_lock?(rs1, :t1)
    refute lm.has_write_lock?(rs2, :t1)
    refute lm.has_write_lock?(rs1, :t2)
    refute lm.has_write_lock?(rs2, :t2)
  end
end
