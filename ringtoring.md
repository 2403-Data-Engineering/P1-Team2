`MATCH (a:Account)-[t:TRANSACTION]->(b:Account) `  
`WHERE a.community_id <> b.community_id AND a.dense_community_flag = 1 AND b.dense_community_flag = 1`  
`RETURN a.community_id, b.community_id, sum(t.amount) as volume, count(t) as tx_count ORDER BY volume DESC`  

This query returns empty, and its not something that can just have the threshold adjusted. This is saying that there is not a single transaction where the sender and recepient are both part of suspicious rings, but also in separate communities. This somewhat makes sense, since there are very few instances of money being sent out of its community to begin with.