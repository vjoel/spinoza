require 'minitest/autorun'
require 'spinoza/system/link'

class TestLink < Minitest::Test
  include Spinoza

  class MockNode
    attr_reader :msgs
    
    def initialize
      @msgs = []
    end
    
    def recv msg: nil
      @msgs << msg
    end
  end

  def setup
    @timeline = Timeline.new
    @node1 = MockNode.new
    @node2 = MockNode.new
    @link = Link[timeline: @timeline,
      src: @node1, dst: @node2, latency: 1.0
    ]
  end
  
  def test_link
    @link.send_message "hello"
    assert_equal [], @node2.msgs
    @timeline.evolve 0.9
    assert_equal [], @node2.msgs
    @timeline.evolve 0.2
    assert_equal ["hello"], @node2.msgs
  end
  
  def test_fifo
    @link.send_message "foo"
    @link.send_message "bar"
    @timeline.evolve 1.0
    assert_equal ["foo", "bar"], @node2.msgs
  end
end
