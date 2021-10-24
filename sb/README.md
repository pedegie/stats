`java -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -jar sb-1.0.jar -c 16 -p 16 -m 100000 -w 3 -d 0.03`

````
asyncprofiler -t -e cpu -f async.svg -d 10 `pgrep -f "sb-1.0.jar"`
````
