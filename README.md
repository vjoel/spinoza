spinoza
=======

A model of the Calvin distributed database.

Spinoza, like Calvin, was a philosopher who dealt in determinism.

The model of the underlying computer and network system is in [lib/spinoza/system](lib/spinoza/system).

Calvin is developed by the Yale Databases group; the open-source releases are at https://github.com/yaledb.

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
