spinoza
=======

A model of the Calvin distributed database.

Spinoza, like Calvin, was a philosopher who dealt in determinism.

The model of the underlying computer and network system is in lib/spinoza/system.

Calvin is developed by the Yale Databases group; the open-source releases are at https://github.com/yaledb.

To do
=====

* The performance and error modeling should optionally be statistical.

* `Log#time_replicated` should be a function of the reading node and depend on the link characteristics between that node and the writing node.

* Transactions, to be more realistic, should have dataflow dependencies among operations. (But only for non-key values, because Calvin splits dependent transsactions.)

* Transactions also need conditionals, or, at least, conditional abort, which is needed to support the splitting mentioned above.

* For comparison, implement a 2-phase commit transsaction processor on top of the Spinoza::System classes.

Contact
=======

Joel VanderWerf, vjoel@users.sourceforge.net, [@JoelVanderWerf](https://twitter.com/JoelVanderWerf).

License and Copyright
========

Copyright (c) 2014, Joel VanderWerf

License for this project is BSD. See the COPYING file for the standard BSD license.
