dsq
===

Distributed Streaming Quantiles

This module implements a count-min sketch-based streaming quantile computation,
as described by [Cormode and Muthukrishnan][1].

It is suitable for working with the Spark execution engine (using the PySpark
API).

### Installation

From the repo:

    git clone https://github.com/laserson/dsq.git
    cd dsq
    python setup.py install





[1]: http://dx.doi.org/10.1016/j.jalgor.2003.12.001
