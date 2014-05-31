require 'minitest/autorun'
require 'spinoza/system/timeline'

class TestTimeline < Minitest::Test
  include Spinoza

  def setup
    @tl = Timeline.new

    @node = {}
    def @node.foo(**h); update h; end
  end
  
  def test_timeline_empty
    assert_equal 0, @tl.now

    assert_nil @tl.step
    assert_equal 0, @tl.now
    
    assert_equal 1.0, @tl.evolve(1.0)
    assert_equal 1.0, @tl.now
  end
  
  def test_schedule_step
    @tl << Event[time: 1.0, actor: @node, action: :foo, x: 1]

    assert_equal 1.0, @tl.step
    assert_equal 1.0, @tl.now
    assert_equal 1, @node[:x]

    assert_nil @tl.step
    assert_equal 1.0, @tl.now
  end

  def test_schedule_evolve
    @tl << Event[time: 1.0, actor: @node, action: :foo, x: 1]
    @tl << Event[time: 2.0, actor: @node, action: :foo, x: 2]
    @tl << Event[time: 3.0, actor: @node, action: :foo, x: 3]

    assert_equal 1.0, @tl.evolve(1.0)
    assert_equal 1.0, @tl.now
    assert_equal 1, @node[:x]

    assert_equal 3.0, @tl.evolve(2.0)
    assert_equal 3.0, @tl.now
    assert_equal 3, @node[:x]
  end
  
  def test_schedule_in_past
    @tl << Event[time: 1.0, actor: @node, action: :foo, x: 1]
    @tl.step
    
    @tl << Event[time: 0.9, actor: @node, action: :foo, x: 2]
    assert_raises Spinoza::Timeline::Error do
      @tl.step
    end
  end
end
