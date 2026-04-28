`MATCH (a:Account)-[t:TRANSACTION]->(b:Account)`
`WITH a.community_id AS community_id,`  
     `sum(CASE WHEN a.community_id = b.community_id THEN t.amount ELSE 0 END) AS intotal,`  
     `sum(CASE WHEN a.community_id <> b.community_id THEN t.amount ELSE 0 END) AS outtotal,`  
     `count(DISTINCT a) AS members`  
`WHERE intotal > 0 AND 3 <= members <= 15`  
`RETURN community_id, members, intotal, outtotal, round(intotal/(outtotal+1), 4) as inoutratio`  
`ORDER BY inoutratio DESC, intotal DESC`  
`LIMIT 500`

This query finds the highest ratios of money sent between members, as well as money going out for each community.  
When calculating the ratio, I add 1 dollar to the outtotal to avoid dividing by zero.  
In the query results there is a jump from 176 million to 200 million in the inout ratio, but this only flags a single community which doesn't seem great, so I'm gonna flag it for inout ratio of over 94million, since the next closest jump is from 94 to 98 million  
Flagging every members in these communities with low membercounts(3-15):

`MATCH (a:Account)-[t:TRANSACTION]->(b:Account)`  
`WITH a.community_id AS community_id,`  
     `sum(CASE WHEN a.community_id = b.community_id THEN t.amount ELSE 0 END) AS intotal,`  
     `sum(CASE WHEN a.community_id <> b.community_id THEN t.amount ELSE 0 END) AS outtotal,`  
     `count(DISTINCT a) AS members`  
`WHERE intotal > 0 AND 3 <= members <= 15`  
`WITH community_id, members, intotal, outtotal, round(intotal/(outtotal+1), 4) as inoutratio`  
`WHERE inoutratio >= 95000000`  

`MATCH (a1:Account)`  
`WHERE a1.community_id = community_id`  
`SET a1.dense_community_flag = 1`  

This query took about 2 minutes, and wrote just over 400 flags 
