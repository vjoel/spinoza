require 'minitest/autorun'
require 'spinoza/calvin/executor'
require 'spinoza/system/store'
require 'spinoza/system/table'
require 'spinoza/transaction'

class TestExecutor < Minitest::Test
  include Spinoza
  
  class MockReadcaster
    def initialize store
      @store = store
      @tables = store.tables
    end
    
    def execute_local_reads txn
      local_read_ops = txn.all_read_ops.select {|r| @tables.include? r.table}
      @store.execute *local_read_ops
    end
    
    def serve_reads txn, local_read_results; end
  end

  def setup
    @store = Store.new(
      Table[:as, id: "integer", name: "string"],
      Table[:bs, id: "integer", name: "string"])

    @executor = Calvin::Executor.new(
      store: @store,
      readcaster: MockReadcaster.new(@store))
  end
  
  def test_passive_txn
    txn = transaction do
      at(:as, id: 1).read
      at(:bs, id: 2).read
      at(:cs).insert id: 3, name: "c3"
      at(:ds, id: 4).read
    end
    
    assert @executor.passive? txn

    result = @executor.execute_transaction txn
    assert result
    assert_equal 2, result.size
    assert_equal txn.ops[0..1], result.map {|r| r.op}
    assert @executor.ready?
  end
  
  def test_txn_with_no_remote_reads
    txn = transaction do
      at(:as).insert id: 1, name: "a1"
      at(:bs, id: 2).read
      at(:cs).insert id: 3, name: "c3"
    end
    
    refute @executor.passive? txn
    assert @executor.all_reads_are_local? txn

    result = @executor.execute_transaction txn
    assert result
    assert_equal 1, result.size
    assert_equal txn.ops[1], result.map {|r| r.op}[0]
    assert @executor.ready?
  end
  
  def test_txn_with_remote_reads
    txn = transaction do
      at(:as).insert id: 1, name: "a1"
      at(:bs, id: 2).read
      at(:cs, id: 3).read
      at(:cs, id: 4).read
      at(:ds, id: 5).read
    end

    refute @executor.passive? txn
    
    result = @executor.execute_transaction txn
    refute result
    refute @executor.ready?
    
    result = @executor.recv_remote_reads :cs, [
      ReadResult.new(val: {id: 3}),
      ReadResult.new(val: {id: 4})
    ]
    refute result
    refute @executor.ready?

    # Same data, different replica:
    result = @executor.recv_remote_reads :cs, [
      ReadResult.new(val: {id: 3}),
      ReadResult.new(val: {id: 4})
    ]
    refute result
    refute @executor.ready?

    result = @executor.recv_remote_reads :ds, [
      ReadResult.new(val: {id: 5})
    ]
    assert result
    assert @executor.ready?
    assert_equal 4, result.size
    
    local_read = result.shift
    refute local_read.val # never wrote this one

    assert_equal Set[3,4,5], Set[*result.map {|r| r.val[:id]}]
  end
end
