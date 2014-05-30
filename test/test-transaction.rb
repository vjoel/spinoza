require 'minitest/autorun'
require 'spinoza/system/node'
require 'spinoza/transaction'

include Spinoza

class TestTransaction < Minitest::Test
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

    assert_equal(1, rslt[0].val.size)
    assert_equal({id: 2, name: "b", len: 3.4}, rslt[0].val[0])

    assert_equal(0, rslt[1].val.size)

    assert_equal(1, rslt[2].val.size)
    assert_equal({id: 3, name: "c", len: 7.8}, rslt[2].val[0])
  end

  def test_read_and_write_sets
    assert_equal({foos: Set[{id: 2}, {id: 3}]}, @txn.read_set)
    assert_equal(Set[:foos], @txn.write_set)
  end
  
  def test_all_read_ops
    assert_equal(3, @txn.all_read_ops.size)
    assert_equal([{id: 2}, {id: 2}, {id: 3}],
      @txn.all_read_ops.map {|op| op.key})
  end
end
