# Copyright 2013 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Distributed streaming quantiles.

This is a Python implementation of distributed streaming quantiles using the
count-min sketch.  It is suitable for using in a distributed computing
environment, like Spark.

"""

import sys
import random
from math import ceil, log, e as euler

def exp2(x):
    return 2.**(x)


class CMSketch(object):
    """Count-min sketch data structure.
    
    As described in Cormode and Muthukrishnan:
    http://dx.doi.org/10.1016/j.jalgor.2003.12.001
    
    Attributes:
        width: (int) width of the sketch
        depth: (int) depth of the sketch (number of hashes)
        hash_state: (tuple of int) defining the set of hash functions. It
            should have depth integers.
    
    """
    
    def __init__(self, width, depth, hash_state):
        """Inits CMSketch with specified width, depth, hash_state."""
        if depth != len(hash_state):
            raise ValueError("depth and len(hash_state) must be equal.")
        self.width = width
        self.depth = depth
        self.hash_state = hash_state
        self._counts = [[0] * self.width for i in xrange(self.depth)]
        self._masks = [CMSketch.generate_mask(n) for n in self.hash_state]
    
    def increment(self, key):
        """Increment counter for hashable object key."""
        for (i, mask) in enumerate(self._masks):
            # hash(key) ^ mask is the i-th hash fn
            j = (hash(key) ^ mask) % self.width
            self._counts[i][j] += 1
    
    def get(self, key):
        """Get estimated count for hashable object key."""
        return min([self._counts[i][(hash(key) ^ mask) % self.width]
                    for (i, mask) in enumerate(self._masks)])
    
    def merge(self, other):
        """Merge other CMSketch with this CMSketch.
        
        The width, depth, and hash_state must be identical.
        """
        self._check_compatibility(other)
        for i in xrange(self.depth):
            for j in xrange(self.width):
                self._counts[i][j] += other._counts[i][j]
    
    def _check_compatibility(self, other):
        """Check if another CMSketch is compatible with this one for merge.
        
        Compatibility requires same width, depth, and hash_state.
        """
        if self.width != other.width or self.depth != other.depth:
            raise ValueError("CMSketch dimensions do not match.")
        if self.hash_state != other.hash_state:
            raise ValueError("CMSketch hashes do not match")
    
    @staticmethod
    def generate_hash_state(num_hashes, seed=1729):
        """Generate some random ints suitable to be a hash_state."""
        random.seed(seed)
        return tuple([random.randint(0, sys.maxint)
                      for i in xrange(num_hashes)])
    
    @staticmethod
    def generate_hash(state):
        """Generate a random hash function, given state (int).
        
        Returns a function that takes a hashable object and returns a hash
        value.
        """
        random.seed(state)
        mask = random.getrandbits(32)
        def myhash(x):
            return hash(x) ^ mask
        return myhash
    
    @staticmethod
    def generate_mask(state):
        """Generate a mask to be used for a random hash fn, given state (int).
        
        Returns mask, which contains random bits. Define a hash fn like so:
        
            def myhash(x):
                return hash(x) ^ mask
        """
        random.seed(state)
        mask = random.getrandbits(32)
        return mask


class QuantileAccumulator(object):
    """Accumulator object for computing quantiles on distributed streams.
    
    This object implements the quantile algorithm using the count-min sketch as
    described in Cormode and Muthukrishnan:
    http://dx.doi.org/10.1016/j.jalgor.2003.12.001
    
    This object requires knowledge of the domain of possible values.  This
    domain is split into dyadic intervals on a binary tree of specified depth
    (num_levels).  The precision of the result depends on the size of the
    smallest dyadic interval over the given domain.
    
    QuantileAccumulator objects can be processed independently and their
    underlying data structures merged, allowing for processing of distributed
    streams.
    
    Attributes:
        total: (int) the total number of objects streamed through
    """
    
    def __init__(self, lower_bound, upper_bound, num_levels, epsilon, delta,
                 seed=1729):
        """Init a QuantileAccumulator with domain and precision information.
        
        The accuracy of the estimator is limited by the size of the smallest
        dyadic subdivision of the domain.  So if the domain is [0, 1], and
        num_levels is set to 10, the smallest subdivision has size 2^(-9).
        
        epsilon should be set to the allowable error in the estimate.  (Note
        that it must be compatible with the num_levels.  If there are not enough
        levels to achieve the necessary accuracy, this won't work.)
        
        delta should be set to the probability of getting a worse estimate
        (i.e., something small, say, 0.05)
        
        The three precision parameters, num_levels, epsilon, and delta
        ultimately define how much memory is necessary to store the data
        structures.  There is one CMSketch per level, and each sketch has a
        width and depth defined by epsilon and delta, as described in the paper.
        
        Args:
            lower_bound: float lower bound of domain
            upper_bound: float upper bound of domain
            num_levels: int number of levels in binary tree dyadic partition of
                the domain.
            epsilon: float amount of error allowed in resulting rank
            delta: float probability of error exceeding epsilon accuracy
            seed: value is fed to random.seed to initialize randomness
        """
        self.total = 0
        
        # domain
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound
        
        # precision/sketch state
        self._num_levels = num_levels
        # width and depth are determined from the Cormode paper, sec. 3.1
        self._width = int(ceil(euler / (epsilon / num_levels)))
        self._depth = int(ceil(log(1. / delta)))
        self._hash_state = CMSketch.generate_hash_state(self._depth, seed)
        self._sketches = [CMSketch(self._width, self._depth, self._hash_state)
                          for i in xrange(self._num_levels)]
    
    def increment(self, value):
        """Increment counter for value in the domain."""
        self.total += 1
        normed_value = float(value - self._lower_bound) / (self._upper_bound -
                                                           self._lower_bound)
        for (level, sketch) in enumerate(self._sketches):
            key = QuantileAccumulator._index_at_level(normed_value, level)
            sketch.increment(key)
    
    def __call__(self, value_iterator):
        """Makes QuantileAccumulator usable with PySpark .mapPartitions().
        
        An RDD's .mapPartitions method takes a function that consumes an
        iterator of records and spits out an iterable for the next RDD
        downstream.  Since QuantileAccumulator is callable, the call can be
        performed like so:
        
            accums = values.mapPartitions(QuantileAccumulator(<parameters>))
            accums.reduce(lambda x, y: x.merge(y))
        
        """
        for value in value_iterator:
            self.increment(value)
        yield self
    
    def merge(self, other):
        """Merge QuantileAccumulator with another compatible one."""
        self._check_compatibility(other)
        for (my_sketch, other_sketch) in zip(self._sketches, other._sketches):
            my_sketch.merge(other_sketch)
        self.total += other.total
    
    def _check_compatibility(self, other):
        """Check if another CMSketch is compatible with this one for merge.
        
        Compatibility requires same domain, precision parameters.
        """
        if (self._lower_bound != other._lower_bound or
                self._upper_bound != other._upper_bound):
            raise ValueError("Quantile domains do not match")
        if self._num_levels != other._num_levels:
            raise ValueError("Number of levels do not match.")
        if (self._width != other._width or self._depth != other._depth
                or self._hash_state != other._hash_state):
            raise ValueError("Sketch parameters do not match.")
    
    def cdf(self, value):
        """Compute estimated CDF at value in domain."""
        _norm = lambda x: float(x - self._lower_bound) / (self._upper_bound -
                                                          self._lower_bound)
        normed_value = _norm(value)
        return self._normed_cdf(normed_value)
    
    def _normed_cdf(self, normed_value):
        """Compute estimated CDF at normed value in [0, 1]."""
        covering = self._get_covering(normed_value)
        accum = 0.
        for segment in covering:
            (level, index) = segment
            accum += self._sketches[level].get(index)
        return accum / self.total
    
    def ppf(self, q):
        """Percent point function (inverse of CDF).
        
        Args:
            q: float in [0, 1].  Lower tail probability.
        
        Returns:
            The value for which q of the observations lie below.  E.g., q = 0.95
            is the 95th percentile.
        """
        _inv_norm = lambda x: x * (self._upper_bound - self._lower_bound) + self._lower_bound
        return _inv_norm(self._binary_search(q))
        
    def _binary_search(self, q, lo=0., hi=1.):
        if hi - lo < exp2(-(self._num_levels + 1)):
            return hi
        mid = lo + (hi - lo) / 2.
        key = self._normed_cdf(mid)
        if key == q:
            return mid
        elif key < q:
            return self._binary_search(q, mid, hi)
        else:
            return self._binary_search(q, lo, mid)
    
    # utilities for working with the binary tree representation of the domain
    
    def _is_leaf(self, level):
        """Is a node at level a leaf node?"""
        return level >= self._num_levels - 1
    
    def _get_covering(self, value, level=0, index=0):
        """Get the set of dyadic ranges that cover [0, value].
        
        value must be a normed value in [0, 1].
        
        Basically this traverses a binary tree where each node has an associated
        domain.  The tree itself doesn't need to be materialized because it can
        be computed using level and index information.
        """
        if (self._is_leaf(level) or
                QuantileAccumulator._value_at_right_boundary(value, level,
                                                             index)):
            return [(level, index)]
        elif (value <= 0 or
                QuantileAccumulator._left_child_contains_value(value, level,
                                                               index)):
            return self._get_covering(value, level + 1, index * 2)
        else:
            return ([(level + 1, index * 2)] +
                    self._get_covering(value, level + 1, index * 2 + 1))
    
    @staticmethod
    def _index_at_level(value, level):
        """Get dyadic range index at given level of binary tree for value.
        
        value is a float in [0, 1] (so required normed values).
        """
        if value <= 0.:
            return 0
        segment_size = exp2(-level)
        index = int(ceil(value / segment_size)) - 1
        return index
    
    @staticmethod
    def _value_at_right_boundary(value, level, index):
        """Is the (normed) value at right boundary of a node in the bin tree?"""
        return value >= (index + 1) * exp2(-level)
    
    @staticmethod
    def _left_child_contains_value(value, level, index):
        return (value > (2 * index) * exp2(-(level + 1)) and 
                value <= (2 * index + 1) * exp2(-(level + 1)))
