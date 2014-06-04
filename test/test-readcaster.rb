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
    
    store = @node.store
    
    op_ia = InsertOperation.new(table: :as, row: {id: 2, name: "a2"})
    op_ib = InsertOperation.new(table: :bs, row: {id: 2, name: "b2"})
    store.execute op_ia, op_ib

    # normally this is part of a Calvin::Node, but here we have a System::Node
    @readcaster = Calvin::Readcaster.new(node: @node)

    @txn = transaction do
      at(:as).insert id: 1, name: "a1"
      at(:as, id: 2).read
      at(:bs, id: 2).read
    end
  end
  
  def test_readcaster
    local_read_results = @readcaster.execute_local_reads @txn
    assert_equal 2, local_read_results.size
    assert_equal "a2", local_read_results[0].val[:name]
    assert_equal "b2", local_read_results[1].val[:name]
  end
  
  def test_serve_reads
    results = []
    class << @readcaster; self; end.send :define_method,
       :send_read do |link, **opts|
      results << [link, opts]
    end
    
    local_read_results = @readcaster.execute_local_reads @txn
    @readcaster.serve_reads @txn, local_read_results

    assert_equal 1, results.size
    link, opts = results.first

    assert_equal @node.links[@na], link
    table, read_results = opts.values_at(:table, :read_results)
    assert_equal :bs, table
    assert_equal 1, read_results.size
    assert_equal "b2", read_results[0].val[:name]
  end
  
  def test_all_reads_local
    assert @txn.all_reads_are_local? @node
    refute @txn.all_reads_are_local? @na
    refute @txn.all_reads_are_local? @nb
  end
  
  def test_remote_read_tables
    assert_equal Set[], @txn.remote_read_tables(@node)
    assert_equal Set[:bs], @txn.remote_read_tables(@na)
    assert_equal Set[:as], @txn.remote_read_tables(@nb)
  end
end
