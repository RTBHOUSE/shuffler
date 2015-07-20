# shuffler

Distributed `shuf` (http://linux.die.net/man/1/shuf). 

Far from optimal, but effective enough for most cases.

## Building

A fat jar:

    mvn clean install assembly:single

## Running

Simple usage:

    hadoop jar shuffler-1.0-SNAPSHOT-jar-with-dependencies.jar com.rtbhouse.shuffler.Shuffler /input /output

Multiple inputs:

    hadoop jar shuffler-1.0-SNAPSHOT-jar-with-dependencies.jar com.rtbhouse.shuffler.Shuffler /input1 /input2 /output

Basic wildcards:

    hadoop jar shuffler-1.0-SNAPSHOT-jar-with-dependencies.jar com.rtbhouse.shuffler.Shuffler /input1/*/* /input2/** /output
