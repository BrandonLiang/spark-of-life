# Spark of Life: Running a genetic algorithm on Spark

This example shows how to handle long running algorithms on Spark. It attempts to find a solution for the travelling
salesman problem: finding the shortest distance while travelling a number of cities while starting and ending in the
same city.

It does this using a Genetic Algorithm inspired by the [talk](https://blog.bwknopper.nl/blog/2016/02/16/jfokus-conference-a-presentation-to-remember/) 
of my friend and colleague [Bas Knopper](https://twitter.com/bwknopper). I won't dive too deep into the algorithm (Bas
does a much better job of explaining it) but it very basically works by generating random routes between cities, finding
the best ones and then recombining and mutating these best ones to find even better ones. This is done in a number of 
iterations.

The algorithm isn't [Embarrassingly Parallel](https://en.wikipedia.org/wiki/Embarrassingly_parallel): when you split the 
work between threads after a while they will start to do a lot of double work. This is why it works in a fork-solve-recombine
fashion where after each iteration a new work package is constructed from the best routes so far. This process continues
 either until the max number of iterations is reached or there is no change in distance anymore between iterations.

## Running

You can easily run the example from your IDE by just running the com.nibado.example.sparkoflife.Main class. It has
sensible defaults for the different command line options but you can always fiddle around with them. It's fun to see
for example the speed difference between 10 and 100 cities.

You should also be able to deploy the jar on a spark cluster after you've built it with "mvn clean install"

## Command line options for com.nibado.example.sparkoflife.Main

| Option | Type   | Default     |Description                        |
|--------|--------|-------------|-----------------------------------|
| -c     | Int    | 100         | Amount of cities to limit to      |
| -i     | Int    | 4           | Number of iterations              |
| -d     | Int    | 8           | Iteration max duration in seconds |
| -s     |        | False       | Shuffle city input                |
| -m     | String | local[4]    | Spark Master                      |
| -a     | String | SparkOfLife | Application name                  |

Example:

-c 50 -s -i 10 -d 4 -m local[2] -a MyApp

This will run the example with 50 cities, shuffled, in 10 iterations max with an iteration duration of 4 seconds max. It 
will run Spark in local mode with 2 workers and name the Spark application MyApp. 


