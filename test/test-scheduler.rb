require 'minitest/autorun'
require 'spinoza/transaction'
require 'spinoza/system/timeline'
require 'spinoza/system/log'
require 'spinoza/system/meta-log'
require 'spinoza/calvin/node'

class TestScheduler < Minitest::Test
  include Spinoza

  def setup
    @timeline = Timeline.new
    @log = Log.new dt_durable: 0.300, dt_replicated: 0.500
    @meta_log = MetaLog.new dt_quorum: 0.300, dt_replicated: 0.500

    @node = Calvin::Node[
      Table[:foos, id: "integer", name: "string"],
      timeline: @timeline,
      log: @log,
      meta_log: @meta_log,
      sequencer: nil, # so the default will get created
      scheduler: nil ## TODO dependency injection
    ]
    
    @dt = @node.sequencer.dt_epoch
  end

  def transaction &b
    Spinoza::Transaction.new &b
  end

  def test_scheduler_without_peers
    @results = []
    @node.on_transaction_finish do |txn, rslt|
      @results << rslt.map {|rr| rr.val} ### add @node.timeline.now to this
      #@node.default_output txn, rslt
    end
    
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

    @node.sequencer.accept_transaction txn1
    @node.sequencer.accept_transaction txn2
    @node.sequencer.accept_transaction txn3

    @timeline.evolve 2.0
    
    assert_empty @results[0]
    assert_equal [{id: 1, name: "a"}], @results[1]
    assert_equal [{id: 1, name: "a"}, nil, {id: 3, name: "cc"}], @results[2]
  end
end
