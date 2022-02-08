# IDRoN, a Clojure Library for Inferring Distance Rankings on Networks


## Synopsis
We deal with the problem of solving a ranking query from a single source to multiple targets.
A ranking is a weak ordering of targets with respect to their distances to the source.
The query is evaluated on a routing network, i.e., a directed graph with a cost function.
For example, this problem translates into finding nearest points of interests (POIs).
Common shortest path searches are time expensive in either pre-calculation or evaluation.
Often, the user is preliminarily satisfied with just the ranking, 
i.e., the distances or the way to follow are secondary information.
Hence, the question arises whether it is possible to find an optimal algorithm for this limited problem.


## Project Goals
We deliver a library that sits on top of Tinkerpop's graph library that
 * generates routing networks based on
   * synthetic data (either internal or external by [Jung](http://jung.sourceforge.net/)
   * real Open Street Map-data
 * provides shortest path solvers (A\* and Dijkstra)
 * uses one of the provided shortest path solvers for our novel algorithm
 * evaluates and tests this algorithm with the [incanter library](http://incanter.org/)


## Implementation
The library is implemented in Clojure 1.5.
We used Clojure with Tinkerpop because
 * Tinkerpop is a great interface for accessing a multitude of graph databases (similar to JDBC for relational databases).
 * Clojure can compile to the JVM.
 * Clojure is a very expressive language that keeps implementation details away from the actual description of the algorithm.
 * Clojure's asynchronous nature is perfect to describe our developed algorithm.


## Examples
 * Run `lein run import /database/mu ~/mu.osm true` to create a fresh Neo4J-database `/database/mu` with data imported from the OSM-XML file `mu.osm`.
 * Run `lein run plot /tmp` to plot an evaluation of the implemented algorithms on some graphs to the directory `/tmp`.
