require 'minitest/autorun'
require 'spinoza/transaction'
require 'spinoza/system/timeline'
require 'spinoza/system/log'
require 'spinoza/system/meta-log'
require 'spinoza/calvin/node'

#===========#
# DEBUGGING #
#===========#
#
# require 'pp'
# pp @results
# pp @timeline.history.select {|time, event| event.action != :step_epoch}

class TestScheduler < Minitest::Test
  include Spinoza
  
  def mkresults node, txn, rslt
    {
      transaction:  txn,
      time:         node.timeline.now,
      values:       rslt.map {|rr| rr.val}
    }
  end
  
  FOOS = Table[:foos, id: "integer", name: "string"]
  BARS = Table[:bars, id: "integer", name: "string"]

  # spec is an array of arrays of Tables, such as [ [FOOS], [FOOS, BARS] ]
  def mknodes spec
    @nodes = spec.map.with_index do |tables, i|
      Calvin::Node[
        *tables,
        name: i,
        timeline: @timeline,
        log: @log,
        meta_log: @meta_log,
        sequencer: nil, # so the default will get created
        scheduler: nil ## TODO dependency injection
      ]
    end

    @results = {}
    @nodes.each do |node|
      rs = @results[node] = []
      node.on_transaction_finish do |txn, rslt|
        rs << mkresults(node, txn, rslt)
        #@node.default_output txn, rslt
      end
    end
  end

  def setup
    @timeline = Timeline.new
    @log = Log.new dt_durable: 0.300, dt_replicated: 0.500
    @meta_log = MetaLog.new dt_quorum: 0.300, dt_replicated: 0.500
  end

  def test_one_node
    mknodes [ [FOOS] ]

    txn1 = transaction do
      at(:foos).insert id: 1, name: "a"
      at(:foos).insert id: 2, name: "b"
      at(:foos).insert id: 3, name: "c"
    end

    txn2 = transaction do
      at(:foos, id: 1).read
      at(:foos, id: 2).delete
      at(:foos, id: 3).update name: "cc"
    end

    txn3 = transaction do
      at(:foos, id: 1).read
      at(:foos, id: 2).read
      at(:foos, id: 3).read
    end

    @nodes[0].sequencer.accept_transaction txn1
    @nodes[0].sequencer.accept_transaction txn2
    @nodes[0].sequencer.accept_transaction txn3

    @timeline.evolve 1.0
    
    rs = @results[@nodes[0]]
    assert_equal [0.81, 0.81, 0.81], rs.map{|r| r[:time]}
    
    assert_empty rs[0][:values]
    assert_equal [{id: 1, name: "a"}], rs[1][:values]
    assert_equal [{id: 1, name: "a"}, nil, {id: 3, name: "cc"}],
      rs[2][:values]
  end

  def test_two_nodes_same_tables
    mknodes [ [FOOS], [FOOS] ]

    txn1 = transaction do
      at(:foos).insert id: 1, name: "a"
      at(:foos).insert id: 2, name: "b"
      at(:foos).insert id: 3, name: "c"
    end

    txn2 = transaction do
      at(:foos, id: 1).read
      at(:foos, id: 2).delete
      at(:foos, id: 3).update name: "cc"
    end

    txn3 = transaction do
      at(:foos, id: 1).read
      at(:foos, id: 2).read
      at(:foos, id: 3).read
    end

    @nodes[0].sequencer.accept_transaction txn1
    @nodes[0].sequencer.accept_transaction txn2
    @nodes[0].sequencer.accept_transaction txn3

    @timeline.evolve 1.0
    
    @results.each do |node, rs|
      desc = node.inspect
      assert_equal [0.81, 0.81, 0.81], rs.map{|r| r[:time]}, desc
    
      assert_empty rs[0][:values], desc
      assert_equal [{id: 1, name: "a"}], rs[1][:values], desc
      assert_equal [{id: 1, name: "a"}, nil, {id: 3, name: "cc"}],
        rs[2][:values], desc
    end
  end

  def test_remote_reads
    mknodes [ [FOOS], [BARS] ]

    @nodes[0].link @nodes[1], latency: 0.010

    txn1 = transaction do
      at(:foos).insert id: 1, name: "a"
      at(:bars).insert id: 2, name: "b"
    end

    txn2 = transaction do
      at(:foos, id: 1).read
        ### TODO use logic or other ops here to make update depend on read
      at(:bars, id: 2).update name: "bb"
        # write must be present or else msg to inactive node is optimized away
    end

    @nodes[0].sequencer.accept_transaction txn1
    @nodes[0].sequencer.accept_transaction txn2

    @timeline.evolve 2.0

    rs = @results[@nodes[1]]

    assert_equal 0.81, rs[0][:time]
    assert_empty rs[0][:values]

    assert_equal 0.81 + 0.010, rs[1][:time]
    assert_equal [{id: 1, name: "a"}], rs[1][:values]
  end

  ### TODO
  ### always assert that results on all nodes are same (if links exist)
  ### test with nontrivial locking, nontrivial Executor concurrency
end
