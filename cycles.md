As far as I can tell, no cycles exist in the database at all, even when not accounting for sequential steps. I didn't get the chance to talk to Kyle about it, so this one is subject to change:

`MATCH path = (a:Account)-[:TRANSACTION*1..8]->(a) RETURN path LIMIT 25`  

This returns no rows, meaning there is not a single cycle from at least sizes 1 to 8 in the database, so I'm just setting the weight for all nodes here to zero.