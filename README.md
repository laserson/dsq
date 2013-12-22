dsq
===

Distributed Streaming Quantiles

This module implements a count-min sketch-based streaming quantile computation,
as described by [Cormode and Muthukrishnan][1].

It is suitable for working with the Spark execution engine (using the PySpark
API).

### Installation

There is currently a very weak dependency on numpy.  It would not be difficult
to remove, at the cost of a bit of performance.

    git clone https://github.com/laserson/dsq.git
    cd dsq
    python setup.py install





[1]: http://dx.doi.org/10.1016/j.jalgor.2003.12.001
