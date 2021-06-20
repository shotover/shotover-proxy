# Redis Benchmarking

## TODO these are wildly out of date
For redis cluster mode support, shotover performs the following throughput:
```
Set - redis proxy:                      146,690.95/s
Get - redis proxy:                      153,558.64/s
Average CPU (325%)
Res: 23716k

Get - redis proxy (no multiplexing):    136,485.33/s
Set - redis proxy (no multiplexing):    121,989.05/s
Average CPU (300%)
Res: 23716k

Set - shotover proxy:                    73,556.31/s
Get - shotover proxy:                   106,397.91/s
Average CPU (205%)
Res: 22764k
```

Running:
3 Masters, 3 Followers