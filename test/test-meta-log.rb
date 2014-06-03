require 'minitest/autorun'
require 'spinoza/system/meta-log'

class TestMetaLog < Minitest::Test
  include Spinoza

  class MockNode
    attr_reader :time_now, :timeline

    def initialize time_now
      @time_now = time_now
      @timeline = [] # note: history and future
    end

    def evolve dt
      @time_now += dt
    end
  end
  
  def setup
    @meta_log = MetaLog.new dt_quorum: 0.300, dt_replicated: 0.500
    @sender = MockNode.new 0.0
    @recver = MockNode.new 0.0
  end
  
  def test_meta_log
    id = @meta_log.append "a", node: @sender

    assert_equal 0.300, @meta_log.time_quorum(id)
    assert_equal 0.500, @meta_log.time_replicated(id)
    
    assert_equal "a", @meta_log.get(id, node: @sender)
    assert_equal nil, @meta_log.get(id, node: @recver)
    
    @sender.evolve 0.200
    refute @meta_log.quorum? id
    @sender.evolve 0.200
    assert @meta_log.quorum? id
    
    @recver.evolve 0.400
    assert_equal nil, @meta_log.get(id, node: @recver)
    @recver.evolve 0.200
    assert_equal "a", @meta_log.get(id, node: @recver)
  end
  
  def test_listeners
    listener = []
    @meta_log.on_entry_available listener, :push
    
    @meta_log.append "a", node: @sender
    @meta_log.append "b", node: @sender
    
    assert_equal 2, @sender.timeline.size

    e = @sender.timeline[0]
    assert_equal @sender.time_now + 0.500, e.time
    assert_equal "a", e.data[:value]

    e = @sender.timeline[1]
    assert_equal @sender.time_now + 0.500, e.time
    assert_equal "b", e.data[:value]
  end
end
