require 'minitest/autorun'
require 'spinoza/calvin/readcaster'
require 'spinoza/system/node'
require 'spinoza/system/link'
require 'spinoza/system/timeline'
require 'spinoza/transaction'

class TestReadcaster < Minitest::Test
  include Spinoza

  def setup
    @timeline = Spinoza::Timeline.new

    @node = Node[
      Table[:as, id: "integer", name: "string"],
      Table[:bs, id: "integer", name: "string"],
      timeline: @timeline
    ]
    
    @na = Node[
      Table[:as, id: "integer", name: "string"],
      timeline: @timeline
    ]

    @nb = Node[
      Table[:bs, id: "integer", name: "string"],
      timeline: @timeline
    ]
    
    @node.link @na, latency: 1.0
    @node.link @nb, latency: 1.0
    
    # normally this is part of a Calvin::Node, but here we have a System::Node
    @readcaster = Calvin::Readcaster.new(node: @node)
  end
  
  def test_readcaster
    store = @node.store
    
    op_ia = InsertOperation.new(table: :as, row: {id: 2, name: "a2"})
    op_ib = InsertOperation.new(table: :bs, row: {id: 2, name: "b2"})
    rslt = store.execute op_ia, op_ib
    
  
    @txn = Transaction.new do
      at(:as).insert id: 1, name: "a1"
      at(:as, id: 2).read
      at(:bs, id: 2).read
    end

    local_read_results = @readcaster.execute_local_reads @txn
    assert_equal 2, local_read_results.size
    assert_equal "a2", local_read_results[0].val[:name]
    assert_equal "b2", local_read_results[1].val[:name]
  end
end
