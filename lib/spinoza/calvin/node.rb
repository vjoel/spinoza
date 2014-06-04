require 'spinoza/system/node'
require 'spinoza/calvin/sequencer'
require 'spinoza/calvin/scheduler'

class Calvin::Node < Spinoza::Node
  attr_reader :sequencer, :scheduler
  attr_reader :log, :meta_log
  
  def initialize *tables, log: nil, meta_log: nil,
      sequencer: nil, scheduler: nil, **rest
    super *tables, **rest

    @log = log
    @meta_log = meta_log
    @sequencer = sequencer || Calvin::Sequencer.new(node: self)
    @scheduler = scheduler || Calvin::Scheduler.new(node: self)

    on_transaction_finish &method(:default_output)
  end

  def default_output transaction, result
    r = result.map {|rr| [rr.op.table, rr.val].join(":")}.join(", ")
    puts "%07.6f [RESULT] #{transaction} => #{r}" % timeline.now
  end

  def recv msg: nil
    scheduler.recv_peer_results **msg
  end
  
  def read_batch batch_id
    log.read batch_id, node: self
  end
  
  def on_transaction_finish &b
    @finished_transaction_handler = b
  end

  # Override this to put the result somewhere.
  def finished_transaction transaction, result
    if @finished_transaction_handler
      @finished_transaction_handler[transaction, result]
    end
  end
end
