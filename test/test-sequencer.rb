require 'minitest/autorun'
require 'spinoza/calvin/sequencer'
require 'spinoza/system/log'
require 'spinoza/system/meta-log'

class TestSequencer < Minitest::Test
  def setup
    @timeline = Timeline.new
    @log = Log.new dt_durable: 0.300, dt_replicated: 0.500
    @meta_log = MetaLog.new dt_quorum: 0.300, dt_replicated: 0.500

    @node = Node[
      Table[:foos, id: "integer", name: "string", len: "float"],
      timeline: @timeline,
      log: @log,
      meta_log: @meta_log
    ]
    
    @dt = 0.010
    @sequencer = Calvin::Sequencer.new node: @node, dt_epoch: @dt
  end
  
  def test_sequencer
    @sequencer.accept_transaction "t1"
    @sequencer.accept_transaction "t2"
    @timeline.evolve @dt

    batch_ids = (0..2).map {|i| @meta_log.get(i, node: @node)}
    assert_equal nil, batch_ids[0]
    assert_equal nil, batch_ids[1]
    assert_equal nil, batch_ids[2]

    @sequencer.accept_transaction "t3"
    @sequencer.accept_transaction "t4"
    @timeline.evolve @dt

    batch_ids = (0..2).map {|i| @meta_log.get(i, node: @node)}
    assert_equal nil, batch_ids[0]
    assert_equal nil, batch_ids[1]
    assert_equal nil, batch_ids[2]

    @timeline.evolve 2.000

    batch_ids = (0..2).map {|i| @meta_log.get(i, node: @node)}
    assert_equal [1, 1], batch_ids[0]
    assert_equal [1, 2], batch_ids[1]
    assert_equal nil, batch_ids[2]
  end
end
