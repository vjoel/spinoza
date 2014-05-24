require 'minitest/autorun'
require 'spinoza/system/node'

include Spinoza

class TestNode < Minitest::Test
  def setup
    @node = Node.new(
      TableSpec.new "foos", id: "integer", name: "string", len: "float"
    )
  end
  
  def test_store
    assert_equal [:foos], @node.store.tables
  end
end
