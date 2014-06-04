require 'minitest/autorun'
require 'spinoza/system/node'
require 'spinoza/transaction'

class TestTransaction < Minitest::Test
  include Spinoza

  def setup
    @node = Node[
      Table[:foos, id: "integer", name: "string", len: "float"]
    ]

    @txn1 = transaction do
      at(:foos).insert id: 1, name: "a", len: 1.2
      at(:foos).insert id: 2, name: "b", len: 3.4
    end

    @txn2 = transaction do
      at(:foos, id: 1).read
      at(:foos, id: 2).read
      at(:foos, id: 3).read
      at(:foos, id: 1).update len: 1.23
      at(:foos, id: 2).delete
      at(:foos).insert id: 3, name: "c", len: 5.6
     end

    @txn3 = transaction do
      at(:foos, id: 1).read
      at(:foos, id: 2).read
      at(:foos, id: 3).read
    end
  end

  def test_txn
    rslt = @node.store.execute(*@txn1.ops)
    assert_equal(0, rslt.size)

    rslt = @node.store.execute(*@txn2.ops)
    assert_equal(3, rslt.size)

    assert_equal({id: 1, name: "a", len: 1.2}, rslt[0].val)
    assert_equal({id: 2, name: "b", len: 3.4}, rslt[1].val)
    assert_equal(nil, rslt[2].val)

    rslt = @node.store.execute(*@txn3.ops)
    assert_equal(3, rslt.size)

    assert_equal({id: 1, name: "a", len: 1.23}, rslt[0].val)
    assert_equal(nil, rslt[1].val)
    assert_equal({id: 3, name: "c", len: 5.6}, rslt[2].val)
  end

  def test_read_and_write_sets
    assert_equal({}, @txn1.read_set)
    assert_equal({foos: Set[Transaction::INSERT_KEY]}, @txn1.write_set)
    assert_equal(Set[], @txn1.read_tables)
    assert_equal(Set[:foos], @txn1.write_tables)

    assert_equal({foos: Set[{id: 1}, {id: 2}, {id: 3}]}, @txn2.read_set)
    assert_equal({foos: Set[Transaction::INSERT_KEY, {id: 1}, {id: 2}]},
      @txn2.write_set)
    assert_equal(Set[:foos], @txn2.read_tables)
    assert_equal(Set[:foos], @txn2.write_tables)

    assert_equal({foos: Set[{id: 1}, {id: 2}, {id: 3}]}, @txn3.read_set)
    assert_equal({}, @txn3.write_set)
    assert_equal(Set[:foos], @txn3.read_tables)
    assert_equal(Set[], @txn3.write_tables)
  end
  
  def test_all_read_ops
    assert_equal(3, @txn2.all_read_ops.size)
    assert_equal([{id: 1}, {id: 2}, {id: 3}],
      @txn2.all_read_ops.map {|op| op.key})
  end
  
  def test_all_write_ops
    assert_equal(3, @txn2.all_write_ops.size)
  end

  def test_active
    node2 = Node[
      Table[:foos, id: "integer", name: "string", len: "float"],
      Table[:bars, id: "integer", name: "string", len: "float"]
    ]
    
    txn = transaction do
      at(:bars).insert id: 1, name: "a", len: 1.2
    end
    
    refute txn.active? @node
    assert txn.active? node2
  end
end
