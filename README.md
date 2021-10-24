# stats

Tracing Java Collections tool

`java -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -jar sb-1.0.jar -c 16 -p 16 -m 100000 -w 3 -d 0.03`

`asyncprofiler -t -e cpu -f async.svg -d 8 `pgrep -f "ForkedMain"`


sudo docker run -p 9090:9090 --network host lossuperktos/stats-prometheus

Prometheus (metrics database) http://localhost:9090