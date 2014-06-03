require 'spinoza/system/model'

# Accepts transaction requests from clients. The requests accepted in an epoch
# are grouped as a batch, given a sequential id, and replicated to the
# transaction schedulers on each node.
class Calvin::Sequencer < Spinoza::Model
  attr_reader :node

  # ID used to construct UUID for batch.
  attr_reader :id
  
  # Length of epoch in seconds.
  attr_reader :dt_epoch

  @seq_id = 0
  class << self
    def next_id
      @seq_id += 1
    end
  end

  def initialize node: raise, dt_epoch: 0.010
    super timeline: node.timeline

    @node = node
    @dt_epoch = dt_epoch
    @batch = []
    @epoch = 0
    @id = self.class.next_id

    step_epoch
  end
  
  def step_epoch
    unless @batch.empty?
      batch_id = [@id, @epoch] # globally unique, but not ordered
      log.write batch_id, @batch, node: node

      log.when_durable batch_id,
        actor: self,
        action: :append_batch_to_meta_log,
        batch_id: batch_id
  
      @batch = []
    end
    @epoch += 1

    timeline.schedule Spinoza::Event[
      time: time_now + dt_epoch,
      actor: self,
      action: :step_epoch
    ]
  end
  
  def append_batch_to_meta_log batch_id: batch_id
    meta_log.append batch_id, node: node
  end
  
  def log
    node.log
  end

  def meta_log
    node.meta_log
  end

  def accept_transaction txn
    @batch << txn
  end
end
