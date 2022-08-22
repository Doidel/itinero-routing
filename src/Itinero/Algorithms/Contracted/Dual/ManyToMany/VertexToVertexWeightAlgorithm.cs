/*
 *  Licensed to SharpSoftware under one or more contributor
 *  license agreements. See the NOTICE file distributed with this work for 
 *  additional information regarding copyright ownership.
 * 
 *  SharpSoftware licenses this file to you under the Apache License, 
 *  Version 2.0 (the "License"); you may not use this file except in 
 *  compliance with the License. You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

using Itinero.Algorithms.Weights;
using Itinero.Data.Contracted;
using Itinero.Graphs.Directed;
using Itinero.Profiles;
using System;
using System.Collections.Generic;
using System.Threading;
using Itinero.Algorithms.Contracted.Dual.Cache;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Itinero.Algorithms.Contracted.Dual.ManyToMany
{
    /// <summary>
    /// An algorithm to calculate many-to-many weights based on a contraction hierarchy between source and target vertices.
    /// </summary>
    public class VertexToVertexWeightAlgorithm<T> : AlgorithmBase
        where T : struct
    {
        protected readonly DirectedMetaGraph _graph;
        protected readonly DykstraSource<T>[] _sources;
        protected readonly DykstraSource<T>[] _targets;
        protected readonly WeightHandler<T> _weightHandler;
        protected readonly T _max;
        protected readonly SearchSpaceCache<T> _cache;

        /// <summary>
        /// Creates a new algorithm.
        /// </summary>
        public VertexToVertexWeightAlgorithm(DirectedMetaGraph graph, WeightHandler<T> weightHandler, DykstraSource<T>[] sources,
            DykstraSource<T>[] targets, T max, SearchSpaceCache<T> cache = null)
        {
            _graph = graph;
            _sources = sources;
            _targets = targets;
            _weightHandler = weightHandler;
            _max = max;
            _cache = cache;
        }

        protected ConcurrentDictionary<uint, ConcurrentDictionary<int, T>> _buckets;
        protected volatile T[][] _weights;

        /// <summary>
        /// Executes the actual run.
        /// </summary>
        protected override void DoRun(CancellationToken cancellationToken)
        {
            // put in default weights, all are infinite.
            // EXPLANATION: a path between two identical vertices has to contain at least one edge.
            _weights = new T[_sources.Length][];
            for (var i = 0; i < _sources.Length; i++)
            {
                _weights[i] = new T[_targets.Length];
                for (var j = 0; j < _targets.Length; j++)
                {
                    _weights[i][j] = _weightHandler.Infinite;
                }
            }

            _buckets = new ConcurrentDictionary<uint, ConcurrentDictionary<int, T>>();

            var parallelOptions = new ParallelOptions()
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = Environment.ProcessorCount - 1
            };

            // do forward searches into buckets.
            Parallel.For(0, _sources.Length, parallelOptions, i =>
            {
                var forward = new Dykstra<T>(_graph, _weightHandler, _sources[i], false, _max, _cache);
                forward.WasFound += (p, v, w) => this.ForwardVertexFound(i, v, w);
                forward.Run(cancellationToken);
            });

            // do backward searches into buckets.
            Parallel.For(0, _targets.Length, parallelOptions, i =>
            {
                var backward = new Dykstra<T>(_graph, _weightHandler, _targets[i], true, _max, _cache);
                backward.WasFound += (p, v, w) => this.BackwardVertexFound(i, v, w);
                backward.Run(cancellationToken);
            });
            
            // invert matrix.
            // TODO: this can be done inplace when matrix is square: https://mouzamali.wordpress.com/2013/06/08/inplace-transpose-a-square-array/
            var transposed = new T[_sources.Length][];
            for (var i = 0; i < _sources.Length; i++)
            {
                transposed[i] = new T[_targets.Length];
                for (var j = 0; j < _targets.Length; j++)
                {
                    transposed[i][j] = _weights[j][i];
                }
            }

            _weights = transposed;

            this.HasSucceeded = true;
        }

        /// <summary>
        /// Gets the weights.
        /// </summary>
        public T[][] Weights => _weights;

        /// <summary>
        /// Called when a forward vertex was found.
        /// </summary>
        /// <returns></returns>
        private bool ForwardVertexFound(int i, uint vertex, T weight)
        {
            _buckets.AddOrUpdate(vertex, (k) => 
            {
                var dict = new ConcurrentDictionary<int, T>();
                dict[i] = weight;
                return dict;
            },
                (k, existingBucket) =>
                {
                    if (existingBucket.TryGetValue(i, out var existing))
                    {
                        if (_weightHandler.IsSmallerThan(weight, existing))
                        {
                            existingBucket[i] = weight;
                        }
                    }
                    else
                    {
                        existingBucket[i] = weight;
                    }
                    return existingBucket;
                });
            return false;
        }

        /// <summary>
        /// Called when a backward vertex was found.
        /// </summary>
        /// <returns></returns>
        private bool BackwardVertexFound(int i, uint vertex, T weight)
        {
            if (!_buckets.TryGetValue(vertex, out var bucket)) return false;

            lock (_weights)
            {
                var weights = _weights[i];
                foreach (var pair in bucket)
                {
                    var existing = weights[pair.Key];
                    var total = _weightHandler.Add(weight, pair.Value);
                    if (_weightHandler.IsSmallerThan(total, existing))
                    {
                        weights[pair.Key] = total;
                    }
                }
            }
            return false;
        }
    }
    
        /// <summary>
    /// An algorithm to calculate many-to-many weights based on a contraction hierarchy between source and target vertices.
    /// </summary>
    public class VertexToVertexWeightAlgorithm : VertexToVertexWeightAlgorithm<float>
    {
        /// <summary>
        /// Creates a new algorithm.
        /// </summary>
        public VertexToVertexWeightAlgorithm(DirectedMetaGraph graph, WeightHandler<float> weightHandler, DykstraSource<float>[] sources,
            DykstraSource<float>[] targets, float max, SearchSpaceCache<float> cache = null)
            : base(graph, weightHandler, sources, targets, max, cache)
        {
            
        }

        private ConcurrentDictionary<uint, ConcurrentDictionary<int, float>> _buckets;
        private volatile float[][] _weights;

        /// <summary>
        /// Executes the actual run.
        /// </summary>
        protected override void DoRun(CancellationToken cancellationToken)
        {
            // put in default weights, all are infinite.
            // EXPLANATION: a path between two identical vertices has to contain at least one edge.
            _weights = new float[_targets.Length][];
            for (var i = 0; i < _targets.Length; i++)
            {
                _weights[i] = new float[_sources.Length];
                for (var j = 0; j < _sources.Length; j++)
                {
                    _weights[i][j] = float.MaxValue;
                }
            }

            _buckets = new ConcurrentDictionary<uint, ConcurrentDictionary<int, float>>();

            var parallelOptions = new ParallelOptions()
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = Environment.ProcessorCount - 1
            };

            // do forward searches into buckets.
            Parallel.For(0, _sources.Length, parallelOptions, i =>
            {
                var forward = new Dykstra<float>(_graph, _weightHandler, _sources[i], false, _max, _cache);
                forward.WasFound += (p, v, w) => this.ForwardVertexFound(i, v, w);
                forward.Run(cancellationToken);
            });

            // do backward searches into buckets.
            Parallel.For(0, _targets.Length, parallelOptions, i =>
            {
                var backward = new Dykstra<float>(_graph, _weightHandler, _targets[i], true, _max, _cache);
                backward.WasFound += (p, v, w) => this.BackwardVertexFound(i, v, w);
                backward.Run(cancellationToken);
            });
            
            // invert matrix.
            // TODO: this can be done inplace when matrix is square: https://mouzamali.wordpress.com/2013/06/08/inplace-transpose-a-square-array/
            var transposed = new float[_sources.Length][];
            for (var i = 0; i < _sources.Length; i++)
            {
                transposed[i] = new float[_targets.Length];
                for (var j = 0; j < _targets.Length; j++)
                {
                    transposed[i][j] = _weights[j][i];
                }
            }

            _weights = transposed;

            this.HasSucceeded = true;
        }

        /// <summary>
        /// Gets the weights.
        /// </summary>
        public new float[][] Weights => _weights;

        /// <summary>
        /// Called when a forward vertex was found.
        /// </summary>
        /// <returns></returns>
        private bool ForwardVertexFound(int i, uint vertex, float weight)
        {
            _buckets.AddOrUpdate(vertex, (k) => 
            {
                var dict = new ConcurrentDictionary<int, float>();
                dict[i] = weight;
                return dict;
            },
                (k, existingBucket) =>
                {
                    if (existingBucket.TryGetValue(i, out var existing))
                    {
                        if (_weightHandler.IsSmallerThan(weight, existing))
                        {
                            existingBucket[i] = weight;
                        }
                    }
                    else
                    {
                        existingBucket[i] = weight;
                    }
                    return existingBucket;
                });
            return false;
        }

        /// <summary>
        /// Called when a backward vertex was found.
        /// </summary>
        /// <returns></returns>
        private bool BackwardVertexFound(int i, uint vertex, float weight)
        {
            if (!_buckets.TryGetValue(vertex, out var bucket)) return false;

            lock (_weights)
            {
                var weights = _weights[i];
                foreach (var pair in bucket)
                {
                    var existing = weights[pair.Key];
                    var total = weight + pair.Value;
                    if (total < existing)
                    {
                        weights[pair.Key] = total;
                    }
                }
            }
            return false;
        }
    }
}