dsq
===

Distributed Streaming Quantiles

This module implements a count-min sketch-based streaming quantile computation,
as described by [Cormode and Muthukrishnan][1].

It is designed with PySpark in mind, but should be suitable with other
distributed computing frameworks (e.g., MapReduce).


### Installation

From the repo:

    git clone https://github.com/laserson/dsq.git
    cd dsq
    python setup.py install


### How it works

The count-min sketch is a probabilistic data structure, analogous to a Bloom
filter that stores counts.  The domain of possible values is split into dyadic
intervals (powers of two) up to a specified number of levels.  (This also means
the domain must be specified in advance.)  Each level has its own count-min
sketch.  When computing a quantile, we estimate the number of observations to
the left of the specified quantile (along with the total count of observations).
The estimate is built by taking the largest possible intervals from the highest
levels of the tree; higher levels are more accurate.


### Example usage

The quantile accumulator requires the specification of a few parameters:

* `lower_bound`, `upper_bound` -- the domain of possible values in the stream

* `num_levels` -- the depth of the binary tree that partitions the domain

* `epsilon`, `delta` -- precision parameters: "the error should be within a
factor of epsilon with probability (1-delta)".  Note that the higher the
demanded precision, the more memory is needed to store the sketch.

Finally, the smallest interval in the dyadic partitioning represents the
smallest precision that is attainable.

```python
# Example PySpark usage

# create a SparkContext
# NOTE: you need to ship the dsq.py file around the cluster
sc = SparkContext("spark://your.spark.master", "YourSparkApp",
        pyFiles=["/path/to/dsq.py"])

# create an RDD containing a bunch of random numbers
import random
values = sc.parallelize([random.uniform(0, 1) for i in xrange(10000)], 5)

# set parameters for QuantileAccumulator
lower_bound = 0
upper_bound = 1
num_levels = 12
epsilon = 0.001
delta = 0.01
seed = 1729 # optional

# create the accumulator function that will process a partition
from dsq import QuantileAccumulator
accum_fn = QuantileAccumulator(lower_bound, upper_bound, num_levels,
        epsilon, delta, seed)

# stream the data through the accumulators
accumulators = values.mapPartitions(accum_fn)
# and merge them all together
quantile_accum = accumulators.reduce(lambda x, y: x.merge(y))

# compute the 95th percentile
quantile_accum.ppf(0.95)
```


[1]: http://dx.doi.org/10.1016/j.jalgor.2003.12.001
