require 'minitest/autorun'
require 'spinoza/system/node'

include Spinoza

class TestNode < Minitest::Test
  def setup
    @node = Node.new(
      TableSpec.new :foos, id: "integer", name: "string", len: "float"
    )
  end
  
  def test_store
    assert_equal [:foos], @node.store.tables
  end
  
  def test_ops
    op_i = InsertOperation.new(table: :foos, row: {id: 1, name: "a", len: 1.2})
    op_r = ReadOperation.new(table: :foos, key: {id: 1})
    rslt = @node.store.execute op_i, op_r
    
    assert_equal(1, rslt.size)
    r_tuples = rslt[0].val
    assert_equal(1, r_tuples.size)
    assert_equal({id: 1, name: "a", len: 1.2}, r_tuples[0])
    
    op_u = UpdateOperation.new(table: :foos, key: {id: 1}, row: {name: "b"})
    rslt = @node.store.execute op_u, op_r
    
    assert_equal(1, rslt.size)
    r_tuples = rslt[0].val
    assert_equal(1, r_tuples.size)
    assert_equal({id: 1, name: "b", len: 1.2}, r_tuples[0])

    op_d = DeleteOperation.new(table: :foos, key: {id: 1})
    rslt = @node.store.execute op_d, op_r
    assert_equal(1, rslt.size)
    r_tuples = rslt[0].val
    assert_equal(0, r_tuples.size)
  end
end
