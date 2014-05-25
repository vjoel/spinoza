require 'minitest/autorun'
require 'spinoza/system/meta-log'

include Spinoza

class TestMetaLog < Minitest::Test
  class MockNode
    attr_reader :time_now

    def initialize time_now
      @time_now = time_now
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
end
