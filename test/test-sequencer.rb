require 'minitest/autorun'
require 'spinoza/system/log'
require 'spinoza/system/meta-log'
require 'spinoza/calvin/node'

class TestSequencer < Minitest::Test
  include Spinoza

  def setup
    @timeline = Timeline.new
    @log = Log.new dt_durable: 0.300, dt_replicated: 0.500
    @meta_log = MetaLog.new dt_quorum: 0.300, dt_replicated: 0.500

    @node = Calvin::Node[
      timeline: @timeline,
      log: @log,
      meta_log: @meta_log,
      sequencer: :none,
      scheduler: :none
    ]
    
    @other_node = Calvin::Node[
      timeline: @timeline,
      log: @log,
      meta_log: @meta_log,
      sequencer: :none,
      scheduler: :none
    ]
    
    @dt = 0.010
    @sequencer = Calvin::Sequencer.new node: @node, dt_epoch: @dt
  end
  
  def test_sequencer
    sid = @sequencer.id
    @sequencer.accept_transaction "t1"
    @sequencer.accept_transaction "t2"
    @timeline.evolve @dt

    batch_ids = (0..2).map {|i| @meta_log.get(i, node: @other_node)}
    assert_equal nil, batch_ids[0]
    assert_equal nil, batch_ids[1]
    assert_equal nil, batch_ids[2]

    @sequencer.accept_transaction "t3"
    @sequencer.accept_transaction "t4"
    @timeline.evolve @dt

    batch_ids = (0..2).map {|i| @meta_log.get(i, node: @other_node)}
    assert_equal nil, batch_ids[0]
    assert_equal nil, batch_ids[1]
    assert_equal nil, batch_ids[2]

    @timeline.evolve @log.dt_durable + @meta_log.dt_replicated - @dt

    batch_ids = (0..2).map {|i| @meta_log.get(i, node: @other_node)}
    assert_equal [sid, 1], batch_ids[0]
    assert_equal nil, batch_ids[1]
    assert_equal nil, batch_ids[2]

    @timeline.evolve @dt

    batch_ids = (0..2).map {|i| @meta_log.get(i, node: @other_node)}
    assert_equal [sid, 1], batch_ids[0]
    assert_equal [sid, 2], batch_ids[1]
    assert_equal nil, batch_ids[2]

    @timeline.evolve 2.000

    batch_ids = (0..2).map {|i| @meta_log.get(i, node: @other_node)}
    assert_equal [sid, 1], batch_ids[0]
    assert_equal [sid, 2], batch_ids[1]
    assert_equal nil, batch_ids[2]
    
    assert_equal ["t1", "t2"], @log.read([sid, 1], node: @node)
    assert_equal ["t3", "t4"], @log.read([sid, 2], node: @node)
  end
end
