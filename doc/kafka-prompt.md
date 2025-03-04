# Kafka Traffic Visualizer

This project P5.js instance mode that simulates a Kafka topic traffic with Producers generating Records with Key/Value and Consumers consuming those Records from the partitions.
Consider it's a single Topic.

Topics:
- Have Partitions

Records:
- Record have Key, keys determine the Partition deterministically with a mod algorithm
- Record have Value, Value varies in bytes size and determine how long they take to Produce to the Partition

Producers:
- They generate Records

Configurations and controls:
- Topic Partition count: 1 to 256, start with 8
- Producers count: 0 to 16, start with 2
- Consumers count: 0 to 256, start with 2

Consumer:
- A Consumer can consume 1 or more Partitions
- Consumes Records in the order of the Partition, it cannot consume out of order

Display and visuals:
- Produce pure P5.js script, no HTMLs or other frameworks allowed
- Partitions should be wide rectangles
- Producers should be on the left and distant from partitions
- a Record is produced to the far left of the partition, presenting an input
- a Record travels within the partition rectangle to the right, where consumers would be
- Records are a ellipses, the ellipse size is proportional to Value size in bytes
- Records have colors which match the Producer color

Metrics:
- Below the Producer, show how many Records were produced, a cumulative sum of Bytes produced and a produce rate in Kilobytes per Second
- Below the Consumer, show how many Records were consumed, a cumulative sum of Bytes consumed and a consume rate in Kilobytes per Second

Physics:
- A Partition behaves like a FIFO queue
- inside a Partition, Records can not move past each other, they have to respect the queue and can only be processed after the previous one is already processed
- Records should accumulate inside a Partition when they are not consumed, they should remain stacked in the order they were produced, like a FIFO queue

Code style:
- Use early returns
- Avoid nested loops, extract functions accordingly with single responsibility principle
- Name variables and functions with Kafka internal terminology and distributed systems paradigms
- Consider performing data structures
- UI/Canvas/Drawing/Controls code should be separated from the Traffic simulation code
- do not pass objects by reference to update them within a function, always return a new object with the updated values
