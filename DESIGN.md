FIFO Queue interface which stores elements on disk. Data
persists between restarts, and (hopefully) crashes.

Can be read by multiple processes. Can be written by 
a single process only.

Thread safe

Access to a queue is managed by a QueueManager which issues
reader and writer interfaces.

A Queue is a directory containing Segments and metadata files
A Segment is a directory containing data and metadata files

Each Segment is made up of
* A data file
* A metadata file 
* A file containing the length of each record
* Possibly a file containing a Checksum for each record
* A file containing information and state about the segment
  * Record count
  * Max Segment Size
  * Remaining Capacity
  * ... 
  
Directory Structure

```
queue-name/      <- queue 
  queue.meta     <- metadata (needed?)
  segment-000/   <- segment
    segment.data <- data
    segment.meta <- metadata
    segment.len  <- data lengths
 segment-001/    <- segment
    segment.data <- data
    segment.meta <- metadata
    segment.len  <- data lengths
 consumer-ABC    <- consumer
 consumer-DEF    <- consumer
```