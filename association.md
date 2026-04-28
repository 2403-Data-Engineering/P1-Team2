`MATCH (a:Account)-[t:TRANSACTION]->(b:Account)`  
`WHERE a.fan_in_flag + a.drain_flag + a.transfer_cashout_flag + a.dense_community_flag = 0 AND b.fan_in_flag + b.drain_flag + b.transfer_cashout_flag + b.dense_community_flag > 0`  
`WITH a, count(DISTINCT b) as bad_neighbors`  
`WHERE bad_neighbors >= 1`  
`RETURN a.id`  

This query returns the unflagged accounts that have a forward transaction with flagged accounts. Ideally it should be 2 as the threshold, but that returned no results since the rest of our thresholds don't have that many rows to begin with. Another case of the paysim dataset not being great.
This flags 1620 accounts as guilty by assocation