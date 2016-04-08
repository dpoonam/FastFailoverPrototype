# FastFailoverPrototype


##dcp_traffic_spy
---

Keeps track of liveliness of the local node (consumer) and remote nodes (producers) that have dcp replication stream open to the local node.

Example of information tracked by dcp_traffic_spy:
> dcp_traffic_spy:get_nodes().
> [{'n_2@127.0.0.1',[{seconds_since_last_update,34.211315},
>                    stale,
>                    {type,producer},
>                    {node_last_heard,{1460,147395,140940}},
>                    {buckets,[{"default",
>                               {bucket_last_heard,{1460,147394,575294}}},
>                              {"test1",{bucket_last_heard,{1460,147395,140932}}}]}]},
> {'n_0@10.17.4.35',[{seconds_since_last_update,34.210637},
>                     stale,
>                    {type,consumer},
>                    {node_last_heard,{1460,147395,141618}},
>                    {buckets,[{"default",
>                               {bucket_last_heard,{1460,147394,575746}}},
>                              {"test1",{bucket_last_heard,{1460,147395,141612}}}]}]}]

## dcp_proxy
---

When dcp_proxy recevies a packet from a node, it calls dcp_traffic_spy:node_alive(Node, [{bucket, Bucket}, {type, Type}]) to update the node liveliveness, if it has not already done so in last one second.

If there is no DCP traffic for sometime, then the producer will send DCP_NOP which will cause the node liveliness to be updated.


##kv_monitor
---

Polls dcp_traffic_spy every one second. If there has been no dcp activity 
for any bucket for greater than 5 seconds, then the state of the node is
 marked as inactive.
If local node is inactive, then it will check ns_memcached:active and warmed 
buckets to get information on ready buckets.


Example:

> kv_monitor:get_nodes().
> [{'n_2@127.0.0.1',[{node_state,inactive},
>                    {buckets,[{"test1",inactive},{"default",inactive}]}]},
> {'n_1@127.0.0.1',[{node_state,inactive},
>                   {buckets,[{"test1",inactive},{"default",inactive}]}]},
> {'n_0@10.17.4.35',[{node_state,active},
>                    {buckets,[{"test1",ready},{"default",ready}]}]}]

