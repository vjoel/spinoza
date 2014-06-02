require 'minitest/autorun'
require 'spinoza/system/node'
require 'spinoza/transaction'

class TestTransaction < Minitest::Test
  include Spinoza

  def setup
    @node = Node[
      Table[:foos, id: "integer", name: "string", len: "float"]
    ]

    @txn = Transaction.new do
      at(:foos).insert id: 1, name: "a", len: 1.2
      at(:foos).insert id: 2, name: "b", len: 3.4
      at(:foos, id: 2).read
      at(:foos).insert id: 3, name: "c", len: 5.6
      at(:foos, id: 2).delete
      at(:foos, id: 2).read
      at(:foos, id: 3).update len: 7.8
      at(:foos, id: 3).read
    end
  end
  
  def test_txn
    rslt = @node.store.execute(*@txn.ops)
    assert_equal(3, rslt.size)

    assert_equal({id: 2, name: "b", len: 3.4}, rslt[0].val)
    refute rslt[1].val
    assert_equal({id: 3, name: "c", len: 7.8}, rslt[2].val)
  end

  def test_read_and_write_sets
    assert_equal({foos: Set[{id: 2}, {id: 3}]}, @txn.read_set)
    assert_equal(Set[:foos], @txn.write_set)
    assert_equal(Set[:foos], @txn.read_tables)
  end
  
  def test_all_read_ops
    assert_equal(3, @txn.all_read_ops.size)
    assert_equal([{id: 2}, {id: 2}, {id: 3}],
      @txn.all_read_ops.map {|op| op.key})
  end
  
  def test_all_write_ops
    assert_equal(5, @txn.all_write_ops.size)
  end

  def test_active
    node2 = Node[
      Table[:foos, id: "integer", name: "string", len: "float"],
      Table[:bars, id: "integer", name: "string", len: "float"]
    ]
    
    txn = Transaction.new do
      at(:bars).insert id: 1, name: "a", len: 1.2
    end
    
    refute txn.active? @node
    assert txn.active? node2
  end
end
