spinoza
=======

A model of the Calvin distributed database. The main purpose of this model is expository, rather than analysis for correctness or performance. All concurrency and distribution is modeled in a single-threaded process with in-memory data tables, which makes it easier to understand what is happening.

Spinoza, like Calvin, was a philosopher who dealt in determinism.

Calvin is developed by the Yale Databases group; the open-source releases are at https://github.com/yaledb.


Structure
=========

The model of the underlying computer and network system is in [lib/spinoza/system](lib/spinoza/system).

The Calvin model, implemented on the system models, is in [lib/spinoza/calvin](lib/spinoza/calvin). Other distributed transaction models could also be implemented on this layer.

The transaction class, in [lib/spinoza/transaction.rb](lib/spinoza/transaction.rb), is mostly abstracted from these layers. It is very simplistic, intended to illustrate Calvin's replication and consistency characteristics.


Running
=======

You will need ruby 2.0 or later, from http://ruby-lang.org, and the gems listed in the gemspec: 

    sequel
    sqlite3
    rbtree

You can also `gem install spinoza`, but it may not be up to date.

To run the unit tests:

    rake test

Examples TBD.

Observing
=========

One benefit of modeling concurrency in a single thread is being able to see what is going on at every node at every moment in time. In fact, Spinoza keeps a history of all "events". (Events are how we model the passage of time and the actions that occur at a point in time.) As noted in [test/test-scheduler.rb](test/test-scheduler.rb), you can use this to make assertions about what occurred, when, and where:

  pp @timeline.history.select {|time, event| event.action != :step_epoch}

(The :step_epoch events are frequent and not usually very interesting.) The hostory is stored in a red-black tree for easy access by time interval.


References
==========

* The Calvin papers:

  * [The Case for Determinism in Database Systems](http://cs-www.cs.yale.edu/homes/dna/papers/determinism-vldb10.pdf)

  * [Consistency Tradeoffs in Modern Distributed Database System Design](http://cs-www.cs.yale.edu/homes/dna/papers/abadi-pacelc.pdf)

  * [Modularity and Scalability in Calvin](http://sites.computer.org/debull/A13june/calvin1.pdf)

  * [Calvin: Fast Distributed Transactions for Partitioned Database Systems](http://www.cs.yale.edu/homes/dna/papers/calvin-sigmod12.pdf)

  * [Lightweight Locking for Main Memory Database Systems](http://cs-www.cs.yale.edu/homes/dna/papers/vll-vldb13.pdf)


To do
=====

* The performance and error modeling should optionally be statistical, with variation using some distribution.

* Model IO latency and compute time, in addition to currently modeled network latency.

* `Log#time_replicated` should be a function of the reading node and depend on the link characteristics between that node and the writing node.

* Transactions, to be more realistic, should have dataflow dependencies among operations. (But only for non-key values, because Calvin splits dependent transactions.)

* Transactions also need conditionals, or, at least, conditional abort, which is needed to support the splitting mentioned above.

* For comparison, implement a 2-phase commit transaction processor on top of the Spinoza::System classes.

* Output spacetime diagrams using graphviz.

* See also 'TODO' in code.


Contact
=======

Joel VanderWerf, vjoel@users.sourceforge.net, [@JoelVanderWerf](https://twitter.com/JoelVanderWerf).

License and Copyright
========

Copyright (c) 2014, Joel VanderWerf

License for this project is BSD. See the COPYING file for the standard BSD license.
