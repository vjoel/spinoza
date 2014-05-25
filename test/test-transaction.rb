require 'minitest/autorun'
require 'spinoza/system/node'
require 'spinoza/transaction'

include Spinoza

class TestTransaction < Minitest::Test
  def setup
    @node = Node.new(
      tables: [
        TableSpec[:foos, id: "integer", name: "string", len: "float"]
      ]
    )
  end
  
  def test_txn
    txn = Transaction.new do
      at(:foos).insert id: 1, name: "a", len: 1.2
      at(:foos).insert id: 2, name: "b", len: 3.4
      at(:foos, id: 2).read
      at(:foos).insert id: 3, name: "c", len: 5.6
      at(:foos, id: 2).delete
      at(:foos, id: 2).read
      at(:foos, id: 3).update len: 7.8
      at(:foos, id: 3).read
    end
    
    rslt = @node.store.execute(*txn.ops)
    assert_equal(3, rslt.size)

    assert_equal(1, rslt[0].val.size)
    assert_equal({id: 2, name: "b", len: 3.4}, rslt[0].val[0])

    assert_equal(0, rslt[1].val.size)

    assert_equal(1, rslt[2].val.size)
    assert_equal({id: 3, name: "c", len: 7.8}, rslt[2].val[0])
  end
end
