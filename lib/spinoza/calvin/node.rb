require 'spinoza/system/node'
require 'spinoza/calvin/sequencer'
require 'spinoza/calvin/scheduler'

class Calvin::Node < Spinoza::Node
  attr_reader :sequencer, :scheduler
  attr_reader :log, :meta_log
  
  def initialize *tables, log: nil, meta_log: nil,
      sequencer: nil, scheduler: nil, **rest
    super *tables, **rest

    @sequencer = sequencer || Calvin::Sequencer.new(node: self)
    @scheduler = scheduler || Calvin::Scheduler.new(node: self)
    @log = log
    @meta_log = meta_log
  end

  def recv **opts
    scheduler.recv **opts
  end
  
  def read_batch batch_id
    log.read batch_id, node: self
  end
  
  def finished_transaction transaction, result
    ### pass result back to client
    puts "[RESULT] #{transaction} => #{result}"
  end
end
