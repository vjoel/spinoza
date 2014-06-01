require 'spinoza/common'
require 'set'

class Calvin::Readcaster
  attr_reader :node
  
  def initialize node: node
    @node = node
    @links = nil
    @tables = node.tables
  end
  
  # Pre-computed map, by table, of which nodes might need data from this node:
  # {table => Set[link, ...]} In other, words, excludes `table,link` pairs for
  # which link.dst already has `table`.
  def links
    unless @links
      @links = Hash.new {|h,k| h[k] = Set[]}
      node.links.each do |there, link|
        (@tables - there.tables).each do |table|
          @links[table] << link
        end
      end
    end
    @links
  end
  
  def execute_local_reads txn
    local_reads = txn.all_read_ops.select {|r| @tables.include? r.table}
    node.store.execute *local_reads
  end
  
  def serve_reads txn, local_read_results
    local_read_results.group_by {|r| r.op.table}.each do |table, results|
      links[table].each do |link|
        if txn.active?(link.dst)
          link.send transaction: txn, table: table, read_results: results
        end
      end
    end
  end
end
