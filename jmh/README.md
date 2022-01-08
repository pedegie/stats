## How to run FlusherProfiler

1. Download [async profiler](https://github.com/jvm-profiling-tools/async-profiler) library
2. Unpack archive
3. Point `LD_LIBRARY_PATH` env variable to `build` directory of unpacked archive  
`export LD_LIBRARY_PATH=~/asyncProfiler/build/`
4. Run main method in `net.pedegie.stats.jmh.profiler.flusher.FlusherProfilerRunner`

*If working on Windows, change your IntelliJ run configuration to execute code in WSL*