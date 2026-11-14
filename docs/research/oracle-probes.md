# turbopuffer oracle probes (2026-11-14)

Raw HTTP exchanges with real turbopuffer, recorded on throwaway `blowfish-oracle-*` namespaces (since deleted). Account-identifying headers removed.

```
### write upsert_rows
HTTP/2 200 
content-type: application/json
x-turbopuffer-billing-bytes-written: 144
x-turbopuffer-created-at: 2026-11-14T01:41:08.533870869Z

{"status":"OK","message":"documents committed successfully","rows_affected":3,"rows_upserted":3,"billing":{"billable_logical_bytes_written":144},"performance":{"server_total_ms":241}}

### ANN
HTTP/2 200 
content-type: application/json

{"rows":[{"$dist":0.003976345,"id":1,"vector":[0.1,0.2,0.3],"n":5,"tags":["a","b"],"text":"the quick brown fox"},{"$dist":0.25464404,"id":3,"vector":[0.0,0.0,1.0],"n":1,"tags":[],"text":"a fox and a dog"},{"$dist":0.52859545,"id":2,"vector":[0.3,0.1,0.0],"n":7,"tags":["b"],"text":"lazy dogs sleep all day"}],"performance":{"cache_hit_ratio":1.0,"cache_temperature":"hot","server_total_ms":12,"query_execution_ms":12,"exhaustive_search_count":3,"approx_namespace_size":3,"last_included_write_at":"2026-11-14T01:41:08.000000000Z"},"billing":{"billable_logical_bytes_queried":1280000000,"billable_logical_bytes_returned":156}}

### BM25+filter
HTTP/2 200 
content-type: application/json

{"rows":[{"$dist":0.49917623,"id":1,"text":"the quick brown fox"},{"$dist":0.45665967,"id":3,"text":"a fox and a dog"}],"performance":{"cache_hit_ratio":1.0,"cache_temperature":"hot","server_total_ms":12,"query_execution_ms":12,"exhaustive_search_count":3,"approx_namespace_size":3,"last_included_write_at":"2026-11-14T01:41:08.000000000Z"},"billing":{"billable_logical_bytes_queried":1280000000,"billable_logical_bytes_returned":58}}

### agg count
HTTP/2 200 
content-type: application/json

{"aggregations":{"c":2},"performance":{"cache_hit_ratio":1.0,"cache_temperature":"hot","server_total_ms":13,"query_execution_ms":13,"exhaustive_search_count":3,"approx_namespace_size":3,"last_included_write_at":"2026-11-14T01:41:08.000000000Z"},"billing":{"billable_logical_bytes_queried":1280000000,"billable_logical_bytes_returned":16}}

### order by n desc
HTTP/2 200 
content-type: application/json

{"rows":[{"id":2},{"id":1}],"performance":{"cache_hit_ratio":1.0,"cache_temperature":"hot","server_total_ms":12,"query_execution_ms":12,"exhaustive_search_count":3,"approx_namespace_size":3,"last_included_write_at":"2026-11-14T01:41:08.000000000Z"},"billing":{"billable_logical_bytes_queried":1280000000,"billable_logical_bytes_returned":16}}

### eventual
HTTP/2 200 
content-type: application/json

{"rows":[{"id":1,"vector":"zczMPc3MTD6amZk+"},{"id":2,"vector":"mpmZPs3MzD0AAAAA"},{"id":3,"vector":"AAAAAAAAAAAAAIA/"}],"performance":{"cache_hit_ratio":1.0,"cache_temperature":"hot","server_total_ms":0,"query_execution_ms":0,"exhaustive_search_count":3,"approx_namespace_size":3,"last_included_write_at":"2026-11-14T01:41:08.000000000Z"},"billing":{"billable_logical_bytes_queried":1280000000,"billable_logical_bytes_returned":60}}

### bad dims
HTTP/2 400 
content-type: application/json

{"error":"💔 Invalid query dimensions! Query has dimensions=1, but index has dimensions=3","status":"error"}

### bad op
HTTP/2 422 
content-type: application/json

{"error":"💔 Failed to deserialize the JSON body into the target type: data did not match any variant of untagged enum FiltersInput at line 1 column 57","status":"error"}

### patch+delete
HTTP/2 200 
content-type: application/json
x-turbopuffer-billing-bytes-written: 36
x-turbopuffer-created-at: 2026-11-14T01:41:08.533870869Z

{"status":"OK","message":"documents committed successfully","rows_affected":2,"rows_patched":1,"rows_deleted":1,"billing":{"billable_logical_bytes_written":36,"query":{"billable_logical_bytes_queried":1280000000,"billable_logical_bytes_returned":49}},"performance":{"server_total_ms":169}}

### after patch
HTTP/2 200 
content-type: application/json

{"rows":[{"id":1,"n":50},{"id":3,"n":1}],"performance":{"cache_hit_ratio":1.0,"cache_temperature":"hot","server_total_ms":10,"query_execution_ms":10,"exhaustive_search_count":5,"approx_namespace_size":5,"last_included_write_at":"2026-11-14T01:41:09.000000000Z"},"billing":{"billable_logical_bytes_queried":1280000000,"billable_logical_bytes_returned":32}}

### metadata
HTTP/2 200 
content-type: application/json

{"schema":{"n":{"type":"int","filterable":true},"text":{"type":"string","filterable":false,"full_text_search":{"k1":1.2,"b":0.75,"k3":8.0,"ascii_folding":false,"case_sensitive":false,"language":"english","stemming":false,"remove_stopwords":false,"tokenizer":"word_v4","max_token_length":39}},"tags":{"type":"[]string","filterable":true},"vector":{"type":"[3]f32","ann":{"distance_metric":"cosine_distance"}},"id":{"type":"uint"}},"approx_row_count":0,"approx_logical_bytes":0,"created_at":"2026-11-14T01:41:08.533870869Z","updated_at":"2026-11-14T01:41:09.000000000Z","last_write_at":"2026-11-14T01:41:09.000000000Z","encryption":{"sse":true},"index":{"status":"up-to-date"}}

### schema
HTTP/2 200 
content-type: application/json

{"vector":{"type":"[3]f32","filterable":null,"full_text_search":null,"ann":true},"tags":{"type":"[]string","filterable":true,"full_text_search":null},"text":{"type":"string","filterable":false,"full_text_search":{"k1":1.2,"b":0.75,"k3":8.0,"ascii_folding":false,"case_sensitive":false,"language":"english","stemming":false,"remove_stopwords":false,"tokenizer":"word_v4","max_token_length":39}},"n":{"type":"int","filterable":true,"full_text_search":null},"id":{"type":"uint","filterable":null,"full_text_search":null}}

### list
HTTP/2 200 
content-type: application/json

{"namespaces":[{"id":"blowfish-oracle-probe1"}],"next_cursor":null}

### no auth bad path
HTTP/2 404 


### multi-query
HTTP/2 200 
content-type: application/json

{"results":[{"rows":[{"$dist":0.0,"id":3},{"$dist":0.19821632,"id":1}]},{"rows":[{"$dist":0.83740485,"id":3},{"$dist":0.19100359,"id":1}]}],"performance":{"cache_hit_ratio":1.0,"cache_temperature":"hot","server_total_ms":11,"query_execution_ms":11,"exhaustive_search_count":5,"approx_namespace_size":5,"last_included_write_at":"2026-11-14T01:41:09.000000000Z"},"billing":{"billable_logical_bytes_queried":2560000000,"billable_logical_bytes_returned":48}}

### rank Sum bm25
HTTP/2 200 
content-type: application/json

{"rows":[{"$dist":0.83740485,"id":3},{"$dist":0.19100359,"id":1}],"performance":{"cache_hit_ratio":1.0,"cache_temperature":"hot","server_total_ms":11,"query_execution_ms":10,"exhaustive_search_count":5,"approx_namespace_size":5,"last_included_write_at":"2026-11-14T01:41:09.000000000Z"},"billing":{"billable_logical_bytes_queried":1280000000,"billable_logical_bytes_returned":24}}

### group by
HTTP/2 200 
content-type: application/json

{"aggregation_groups":[{"c":1,"n":1},{"c":1,"n":50}],"performance":{"cache_hit_ratio":1.0,"cache_temperature":"hot","server_total_ms":16,"query_execution_ms":16,"exhaustive_search_count":5,"approx_namespace_size":5,"last_included_write_at":"2026-11-14T01:41:09.000000000Z"},"billing":{"billable_logical_bytes_queried":1280000000,"billable_logical_bytes_returned":48}}

### explain
HTTP/2 400 
content-type: application/json

{"error":"💔 index does not exist, cannot explain","status":"error"}

### conditional upsert
HTTP/2 200 
content-type: application/json
x-turbopuffer-billing-bytes-written: 0
x-turbopuffer-created-at: 2026-11-14T01:41:08.533870869Z

{"status":"OK","message":"documents committed successfully","rows_affected":0,"billing":{"billable_logical_bytes_written":0,"query":{"billable_logical_bytes_queried":1280000000,"billable_logical_bytes_returned":16}},"performance":{"server_total_ms":108}}

### delete_by_filter
HTTP/2 200 
content-type: application/json
x-turbopuffer-billing-bytes-written: 20
x-turbopuffer-created-at: 2026-11-14T01:41:08.533870869Z

{"status":"OK","message":"documents committed successfully","rows_affected":1,"rows_deleted":1,"billing":{"billable_logical_bytes_written":20,"query":{"billable_logical_bytes_queried":1280000000,"billable_logical_bytes_returned":8}},"performance":{"server_total_ms":966}}

### delete ns
HTTP/2 200 
content-type: application/json

{"status":"OK"}

### list
HTTP/2 200 
content-type: application/json

{"namespaces":[],"next_cursor":null}

```

Earlier probe: a write mixing `uint` and string IDs in one batch returned 400 `{"error":"💔 namespace ID type is uint, but you sent string; you are not allowed to mix ID types in the same namespace","status":"error"}` and wrote nothing. Queries on a missing namespace return 404 `{"error":"🤷 namespace 'NAME' was not found","status":"error"}`.
