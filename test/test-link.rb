require 'minitest/autorun'
require 'spinoza/system/link'

include Spinoza

class TestLink < Minitest::Test
  def setup
    @node1 = Node[
      Table[:foos, id: "integer", name: "string", len: "float"]
    ]
    @node2 = Node[
      Table[:foos, id: "integer", name: "string", len: "float"]
    ]
    @link = Link[
      src: @node1, dst: @node2, latency: 1.0
    ]
  end
  
  def test_link
    @link.send "hello"
    assert_nil @link.recv
    @node2.evolve 0.9
    assert_nil @link.recv
    @node2.evolve 0.2
    assert_equal "hello", @link.recv
    assert_equal nil, @link.recv
  end
  
  def test_fifo
    @link.send "foo"
    @link.send "bar"
    @node2.evolve 1.0
    assert_equal "foo", @link.recv
    assert_equal "bar", @link.recv
    assert_equal nil, @link.recv
  end
end
