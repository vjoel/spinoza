require 'minitest/autorun'
require 'spinoza/system/log'

include Spinoza

class TestLog < Minitest::Test
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
    @log = Log.new dt_durable: 0.300, dt_replicated: 0.500
    @sender = MockNode.new 0.0
    @recver = MockNode.new 0.0
  end
  
  def test_log
    @log.write "a", 1, node: @sender
    assert_raises Log::KeyConflictError do
      @log.write "a", 1, node: @sender
    end

    assert_equal 0.300, @log.time_durable("a")
    assert_equal 0.500, @log.time_replicated("a")
    
    assert_equal 1, @log.read("a", node: @sender)
    assert_equal nil, @log.read("a", node: @recver)
    
    @sender.evolve 0.200
    refute @log.durable? "a"
    @sender.evolve 0.200
    assert @log.durable? "a"
    
    @recver.evolve 0.400
    assert_equal nil, @log.read("a", node: @recver)
    @recver.evolve 0.200
    assert_equal 1, @log.read("a", node: @recver)
  end
end
