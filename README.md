# Stats

**Stats** is Java tracing collection library. Stats allows monitoring yours `Collection` with zero-cost overhead.

## Overview  
**Stats** is lightweight `Collection` (and its children, like `Queue` / `List` / etc.) Decorator which saves to file 
current `size` of `Collection` on every each access if it changes its size - on reading side library can push data to
monitoring tools like [Graphite](https://graphiteapp.org/), [ElasticSearch](https://www.elastic.co/) or makes data 
available to [Prometheus](https://prometheus.io/) and other pull-based inspection tools.  

Library is build on top of [Chronicle-Queue](https://github.com/OpenHFT/Chronicle-Queue) which gives us huge advantage 
of super low latency and persistent data if crash happens or if we want to postpone publishing collected statistics.  

## Motivation 
The problem I have to deal with almost all my projects was: "How to monitor **efficiently** thread pools, without 
*observer-effect*?". I believe thread pools are the most important parts in correctly designed system, when it comes
to monitoring, collecting statistics, scaling, debugging and bottleneck recognition.

When you design your system, there are (or at least should be) separate thread pools for different responsibilities:
thread pool which connects to database, services which asks network for data also usually need a thread pool, 
for example most of HTTP client libraries use thread pools, some CPU-bound workload delegated to separate pool or just 
simple Akka actor consuming messages from queue which is served by Dispatcher. We can say, that our system is divided
into multiple parts separated by thread pools. What if we could get notification each time when pool is saturated? It
then means that **this** part of system is overload, additionally with real-time monitoring, we can just prevent
overloading system by taking some actions **before** system becomes unresponsive or in the worst case crash.    

Another situation is: system failure - client says that something is not working, lets say users cannot log in to site,
and we don't see any errors, or other team within our company say than sometimes they get disconnects from our service 
with no reason - these problems are really hard to diagnose when it just slowdowns without any explicit reason.  
Typically, in kind of these problems we dive into investigating our machine - checking RAM, CPU, network, 
file descriptors, thread pools, it may be necessary to run some tracing / profiling tools too - which is not always
the case especially in production systems. If we think about it a little more - we do it all in one reason: link 
hardware problems to source code, when we find this place, we can read the code, determine what the problem is and
propose solution, for example our outbound network interface is overloaded, by continuously sending huge packages, what
shouldn't happen, we can use tools like `bpftrace` to capture packages which size is higher than X, find port and 
process related to it, then try to resolve a stacktrace in hope we get into right place in source code, or 
high CPU usage, because of our GC is taking all CPU time performing full GCs, memory dump shows as there is a lot of
`byte[]` arrays with given size, we can then look for these sizes in our app, but it's usually not that simple and
in the best case a lot of time-consuming. The real problem is, that on hardware / OS level, we don't know anything
about application logic - so we spend most time for linking one to the other. In both cases there is really huge chance
we can find problem just looking for saturated thread pools, in first case it would be some pool of network client, 
which continuously sends requests, in second case maybe a lot of messages stays on our `Queue` because consumer isn't 
fast enough.  

I need variance, not just average - information that there is on average 100 000 elements in queue per hour is
not enough, we can omit peaks, what if there was peak 95 000 elements in first minute of hour? Without this information
we cannot really find the problem in case of system failure, because it may be caused by this peak. **Stats** records
every single element, or at least enough to determine this situation. It's why, one of main goals of **Stats** is
to be super fast, last thing we would like to have is overloading already overloaded system but at the same time,
tracing overloaded system gives us the most information, **Stats** is doing it with zero-cost.  

So basically, why it's `Collection` Decorator, and not some "Thread Pool Stats"? Well, it's because of how Java thread
pools works. When there isn't enough threads to do the job, and max limit of threads in pool has been reached, items
go on queue, we can say, that thread pool is saturated when it contains at least single element - it's why monitoring 
of queue is the key. So if I'm going to create a queue monitoring library, why not to extend this to `Collection`
interface? There is a lot of situations, when we may need, trace other collections too. 

**Stats** is designed to do one job but really well and with this thought it was developed. All source code changes
which makes **Stats** slower, will be rejected - also all merge requests which proves with benchmarks, that makes
it faster without breaking functionality and extensibility - accepted.

## Architecture
**Stats** can be divided into two parts - write side, which decorates `Collection` responsible for writing to file and
read side which reads probes from file and publish data to monitoring tools. **Stats** is build on top of
[Chronicle-Queue](https://github.com/OpenHFT/Chronicle-Queue) what means, both sides access *Memory Mapped File*, instead of 
interacting with physical storage, what makes it really fast. On reading side, there is `ProbeTailerScheduler` which schedules
reading probes by `ProbeTailers`.

![Architecture image](images/architecture.png "Architecture")

1. Writers and ProbeTailers are independent of each other. **They can be separate processes**.
2. There may be at most one writer for each file. 
3. There may be multiple ProbeTailers for single file - in case you want to publish data to multiple sources.
4. ProbeTailers can work within scheduler or outside, advantage of using scheduler is that one thread can handle multiple tailers.
5. If you run both sides at the same time, there is no physical storage access overhead, thanks to memory mapped files.
6. You can write probes to file, and collect / analyze them later, maybe from another process.

Writing probes is [producer centric](https://github.com/OpenHFT/Chronicle-Queue#what-is-a-producer-centric-system) 
what means, that "fast producer, slow consumer" problem doesn't exist at all. Reading and writing can be doing from 
different processes - again thanks to [Chronicle-Queue](https://github.com/OpenHFT/Chronicle-Queue).  
  
One of main **Stats** goals, is to allow easy expandability:
- adding new integration with monitoring tool should be easy as possible - [add new integration]()
- changes to most important parts of core **Stats** mechanisms, should be easily by just providing custom implementation -
what basically means, you can just plug-in your own `Probe` serializer / deserializer, compression mechanism, `Probe`
filter, exception handler any many more -  [interference into **Stats**]()

### Zero-cost
What basically means zero-cost? Well in short it's - "if writing probe is going to have any impact - don't do it". 
Does it mean probe is lost then? No, lets explain how actually writing works. 

##### Concurrent Collections:  
When you add or removes something from `Collection`, its `size` is kept in [LongAdder](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/atomic/LongAdder.html).
Next, the writing thread have to win Compare and Swap instruction to exclusively access [Bytes](https://www.javadoc.io/doc/net.openhft/chronicle-bytes/1.12.17/net/openhft/chronicle/bytes/Bytes.html)
where it puts current timestamp and `size` computed with `LongAdder.intValue()`. Bytes contain batched data, 
later flushed (see [Batching]()) by `Flusher`. In case of  contention, thread just lose CAS but this change is still 
registered in `LongAdder`, so on next write its taken into account. Mechanism is `lock-wait free` we say.

##### Non-Concurrent Collections: 
We can configure **Stats** to completely disable synchronization mechanisms - its useful when we don't work within
concurrent environment, or there is a guarantee that at most one thread at given time access `Collection` - for example
Akka's Actor could have some `Map` or `List`, and as we know, Actors are thread safe by design. In this scenario we
should set parameter `disableSynchronization` to true during creating `StatsQueue`. 

Writing to non-concurrent collections is pretty same as with concurrent but instead of `LongAdder` there is simple `long`,
also CAS instruction is removed what gives a little better performance.

## Implementation Details  

**Stats** keeps both probes in memory mapped file and batched data on off-heap, so it's not affected by Garbage Collector
at all - there is no risk, that after adding **Stats** library, it increases GC impact.  

Probes are stored in simple format, which consists of pairs `[ [ timestamp, size ], [ timestamp, size ] ]`, there is
coming `CompressedProbeAccess` which handles it little differently - it puts starting timestamp in first probe record
and then every next probe, just contains a difference between "now" and starting timestamp, so we can keep timestamps in
`int` instead of `long` - and its huge boost, due to less frequent requirement for pre-touching memory mapped files, 
obvious bonus is this format just takes less space.  

#### Flusher
`Flusher` is a thread working in background. His responsibility is to flush data batched by all writers in case 
they contain some stale data. For example, we can configure `Batching` to flush every N probes or time threshold (see [Batching]()).
Time threshold is crucial, because until we flush, `ProbeTailer` don't see any probes so in case we are in "half of batch" state
and there isn't any more incoming probes to trigger threshold, we need some utility to flush the batch - that's what `Flusher` is for.

`Flusher` is designed to take CPU resources only when it needs to do some work. Every **Stats** decorated `Collection`, pass
`flushMillisThreshold` with its `Batching` configuration. `Flusher` keeps these timestamps in `PriorityQueue` and sleeps, until
head of queue is eligible for flush giving back CPU resources. `Flusher` is also *lock-wait free* - when it wakes up, it checks
if head of queue representing `Collection` wasn't flushed in meanwhile. If it was its just re-inserted into `PriorityQueue` 
otherwise few attempts (separated in time) are taken to flush data. **"Attempts"** because of the same *zero-cost* rule described 
for writers mechanism. `Flusher` challenges the same synchronizer with CAS instruction, if it lost few times flushing is postponed
to next interval. In case of high contention, we won't put additional pressure on system also there is really high chance that
during writing probes (and writes actually happens, because we lost CAS) it reaches `batchSize` and get flushed anyway.  

#### Strong eventual consistency
It's worth to mention that counting probes has `SEC` semantics. Multiple threads can access `LongAdder.add(n)` and they
get eventually consistent during `LongAdder.intValue()` within thread, which win CAS challenge - keep it in mind in case of 
debugging.

## Code & Features

### Writer

Decorating `Collection` requires providing two: `Configuration` and `Collection`. (only `Queue` interface supported for now).  

Minimal configuration is providing `Path` representing probes log file, but I highly recommend considering other configuration
properties if defaults don't suit you. `Path` should match for both sides - writer and `ProbeTailer`.
```java
Queue<Integer> queue = new ConcurrentLinkedQueue<>();

QueueConfiguration queueConfiguration = QueueConfiguration.builder()
        .path(Paths.get("probes.log"))
        .build()

queue = StatsQueue.from(queue, queueConfiguration);
queue.add(5);
```

#### mmapSize 
`mmapSize` is size of memory mapped file in bytes, the higher value the less frequent re-mapping will be. Remapping
and pre-touching is done in background. In general, higher value is better but take care if you have enough RAM.
1 MB can hold 87381 probes or 131072 when using `CompressedProbeAccess`.

default: 5MB
```java
QueueConfiguration queueConfiguration = QueueConfiguration.builder()
        .path(Paths.get("probes.log"))
        .mmapSize(1024 * 1024 * 512) // 512 MB
        .build()
```
#### rollCycle
See [RollCycle](https://github.com/OpenHFT/Chronicle-Queue#detailed-guide)  
default: `RollCycles.DAILY`
```java
QueueConfiguration queueConfiguration = QueueConfiguration.builder()
        .path(Paths.get("probes.log"))
        .rollCycle(RollCycles.HOURLY)
        .build()
```
#### disableSynchronization
disables all synchronization mechanisms giving a bit of performance, use only if there is guarantee that at most one
thread access `Collection` at any time. It doesn't have to be **the same** thread, just **only one at given time**.  

default: `false`
```java
QueueConfiguration queueConfiguration = QueueConfiguration.builder()
        .path(Paths.get("probes.log"))
        .disableSynchronization(true)
        .build()
```
#### preTouch
determines whether memory mapped file should be pre-touched when `StatsQueue` is created. Usually it means, that pre-touching overhead
is moved to *load-time* instead of *runtime*.  

default: `true`
```java
QueueConfiguration queueConfiguration = QueueConfiguration.builder()
        .path(Paths.get("probes.log"))
        .preTouch(false)
        .build()
```
#### writeFilter
`WriteFilter` allows you to filter out unnecessary writes overhead in case when you are not interested in current `StatsQueue` state.
For example, you might want to gather statistics only when `Queue` `size` is greater than 500. [It doesn't mean probes are lost!]
(zero-cost section). `WriteFilter` is simple interface:
```java
@FunctionalInterface
public interface WriteFilter
{
    boolean shouldWrite(int size, long timestamp);

    static WriteFilter acceptAllFilter()
    {
        return (size, timestamp) -> true;
    }

    static WriteFilter acceptWhenSizeHigherThan(int size)
    {
        return (sizee, timestamp) -> sizee > size;
    }
}
```
Let's say, we don't care until there is less than 3 elements, so we can provide this configuration:
```java
QueueConfiguration queueConfiguration = QueueConfiguration.builder()
        .path(Paths.get("probes.log"))
        .writeFilter(WriteFilter.acceptWhenSizeHigherThan(2))
        .build()

queue = StatsQueue.from(queue, queueConfiguration);
queue.add(5);     // []
queue.add(2);     // []
queue.add(50);    // [1638565640, 3]   first value is sample timestamp generated during writing 3rd probe, second value means current size of queue
```
You can provide custom `WriteFilter` implementation as you wish.  

default: `WriteFilter.acceptAllFilter();`

#### errorHandler
`FileAccessErrorHandler` is another simple interface, which allows defining behaviour in case of exception throws during
write to or closing file. 
```java
public interface FileAccessErrorHandler
{
    /**
     * @param throwable t
     * @return true if queue should be closed, false otherwise. Default is false
     */
    default boolean onError(Throwable throwable)
    {
        return false;
    }
}
```
If error occurs during closing file, then return value is ignored, avoiding infinity loop. `StatsQueue` goes into `CLOSE_ONLY`
state, see [closing file](). You can provide your own implementation as you wish.  

default: `FileAccessErrorHandler logAndIgnore()` - just logs and return `false`
```java
QueueConfiguration queueConfiguration = QueueConfiguration.builder()
        .path(Paths.get("probes.log"))
        .errorHandler(new MyCustomErrorHandler())
        .build()
```
#### probeAccess
`ProbeAccess` is low-level interface, responsible for reading and writing probes. If you want create your own `Probe`
de/serialization logic or trace every each write / read - its place to go.  

default: `ProbeAccess.defaultAccess()` - its reference to currently developed best `ProbeAccess`, now its `DefaultProbeAccess` 
which just put `long timestamp` and `int size` as is but future implementations will be more efficient 
(for example `CompressedProbeAccess` described above)
```java
QueueConfiguration queueConfiguration = QueueConfiguration.builder()
        .path(Paths.get("probes.log"))
        .probeAccess(new MyCustomProbeAccess())
        .build()
```
#### writeThreshold
`WriteThreshold` is really useful when you expect huge load like tens of millions probes per second. It allows you
give lower bounds conditions for writing probes and still keep negligible overhead equal to just single 
`System.currentTimestampMillis()` call. It takes two parameters:  
`minDelayBetweenWritesMillis` - determines minimum delay between two writes in millis  
`minSizeDifference` - determines minimum size difference which queue must change  
If any of above is met - writing process continues.  
Let's say incoming load is 100k probes per second on average, if you set `minDelayBetweenWritesMillis` to 100 there 
will be only 10 writes per second on average instead of 100k - huge boost! But what if we are streaming live data
to our monitoring system, and we are really interested in both: peaks when collection size significantly increased
(`queue.addAll(n)`, where n contains many elements) or opposite (`queue.clear()`) - in both cases, it may be worth to 
notice that change, maybe they happen periodically? It's when `minSizeDifference` comes into play. If set to 20 and
`queue.clear()` removes at least 20 elements, then write is forced - it refers to all `Queue` methods which allows
modifying size by more than one element.  
 
Can't we do that with `WriteFilter`? Sure we can, but note than `WriteFiter` already takes `size` as argument 
what means we have to compute that invoking `LongAdder.intValue()` and win CAS race because `LongAdder` computation
happens only after successful CAS. Both gives some overhead  which may be significant when dealing with huge loads.
If we can decide earlier to skip write it's better to do this earlier. If we discard write request at `WriteThreshold`
whole overhead is just `LongAdder.add()` (in non-synchronized mode just `i++` instead) and `System.currentTimeMillis()`
call. Does it mean probes are dropped then? - [again no]().  

default: `minDelayBetweenWritesMillis` - 5000, `minSizeDifference` - 1  
What means our lower bounds for accepting writes is at most one write per 5 seconds regardless of `Queue` `size` 
change difference.
```java
QueueConfiguration queueConfiguration = QueueConfiguration.builder()
        .path(Paths.get("probes.log"))
        .writeThreshold(WriteThreshold.minSizeDifference(20))
        .build()
```
#### batching

`Batching` in simple words is a tradeoff between faster writes and data visibility by `ProbeTailer`. It takes two
parameters:  

`flushMillisThreshold` - maximum interval in millis between flushes, it forces data if flushed at least once per threshold   
`batchSize` - how many probes we can batch before flush happens    

We need both, to ensure that `ProbeTailer` will ever see actual data. See [Flusher section]() for detailed 
explanation how flushing works. Basically if you don't expect to push / expose your data to monitoring tools 
more often than 1 minute set `flushMillisThreshold` to `60 * 1000`. `batchSize` should be big enough to store all
probes incoming within `flushMillisThreshold`. Also keep in mind that batched data also
takes RAM space - its stored on off-heap.  

default: `flushMillisThreshold` - 5000, `batchSize` - 50
```java
QueueConfiguration queueConfiguration = QueueConfiguration.builder()
        .path(Paths.get("probes.log"))
        .batching(new Batching(60_000, 200))
        .build()
```
### ProbeTailer
`ProbeTailer` represents read-side, creating `ProbeTailer` requires two: `Path` and `Tailer`
```java
@FunctionalInterface
public interface Tailer
{
    void onProbe(Probe probe);

    default void onClose() {}
}
```
**Until you are not [creating new integration]() you shouldn't implement your own `Tailer`**. It's low-level interface 
required to integrate **Stats** with some monitoring tools, like Graphite or Prometheus. In most cases you should use 
[ready-made Tailers](). Tailers remember their last read position index, so they can continue from there after restart.
You can read more about implementation details here: [Chronicle-Queue tailer](https://github.com/OpenHFT/Chronicle-Queue#reading-from-a-queue-using-a-tailer)

```java
TailerConfiguration configuration = TailerConfiguration.builder()
        .tailer(tailer)
        .path(Paths.get("probes.log"))
        .build();

ProbeTailer probeTailer = ProbeTailer.from(configuration);
probeTailer.read(50);         // reads 50 probes
probeTailer.read();           // read all available probes
probeTailer.readFromStart();  // read all probes from beginning
probeTailer.probes();         // returns available probes to read
probeTailer.close();          // close tailer
```
`TailerConfiguration` takes **probeAccess**, **mmapSize** and **batchSize** parameters described above.
The only difference is there is no `flushMillisThreshold` for batching configuration because it makes
no sense here as there isn't any flushing mechanic. `batchSize` determines how often page fault can happen.
Let's say there is already 30 batched probes and `read(50)` is invoked. It takes 30 probes from batch `Bytes` 
then it asks *memory mapped file* for next 50 probes, it takes as much as there is up to 50 and continue
reading from next batch `Bytes` slice. 

### ProbeTailerScheduler
Continuously reading probes written by write side requires to program some loop invoking `ProbeTailer`
read methods periodically. `ProbeTailerScheduler` do this for you. Internally its just wrapper for 
[Chronicle VanillaEventLoop](https://github.com/OpenHFT/Chronicle-Threads)

`ProbeTailerScheduler` takes 2 parameters:  
- `threads` - how many event loops to create, single event loop can handle multiple tailers, recommended value
is just single thread, it should be enough for most cases but test it.
- `probeReadOnSingleAction` - scheduler invokes `ProbeTailer.read(n)` method, parameter specifies `n` value.

default: `threads` is 1, `probeReadOnSingleAction` is 50

```java
TailerConfiguration configuration1 = TailerConfiguration.builder()
        .tailer(tailer)
        .path(Paths.get("probes_1.log"))
        .build();

TailerConfiguration configuration2 = TailerConfiguration.builder()
        .tailer(tailer)
        .path(Paths.get("probes_2.log"))
        .build();

ProbeTailer probeTailer1 = ProbeTailer.from(configuration1);
ProbeTailer probeTailer2 = ProbeTailer.from(configuration2);

ProbeTailerScheduler scheduler = ProbeTailerScheduler.create();
scheduler.addTailer(tailer1);
scheduler.addTailer(tailer2);

scheduler.close();
```


// closing opisac

// dopisac jaki tradeoff kazdy parametr robi i co sie zaleca w jakiej sytuacji
// tailerze pamietac zeby opisac feature, a pozniej jak to zsetupowac obydwa tez przyklady kodu ze schedulerem
// feature'y wszystkie opisac, w osobnej sekcji dodac todo feature'y oraz liste obslugiwanych kolekcji
// supported monitoring tools
// kiedy nie uzywac, np dla dokladnych statystyk
// benchmarki
// spis tresci dodac
// moze jeszcze opisac mechanizm close mode | co sie stanie w razie awarii roznych (to moze pominac?)
// dlaczego nie jmx
