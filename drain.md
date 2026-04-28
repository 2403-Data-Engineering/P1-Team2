`MATCH (a)-[t1:TRANSACTION]->(b), (b)-[t2:TRANSACTION]->(c)`  
`WHERE t2.step > t1.step AND t2.step - t1.step <= 2 AND t2.newbalanceOrig < (t1.amount * 0.1)`  
`RETURN DISTINCT b.id`  
This query returns every account that received money, then nearly emptied their account within the next 2 steps  

The range of 2 is adjustable, right now it returns 62 accounts which I think is fine. So we can write the drain flag weight to each of these accounts
`MATCH (a)-[t1:TRANSACTION]->(b), (b)-[t2:TRANSACTION]->(c)`  
`WHERE t2.step > t1.step AND t2.step - t1.step <= 2 AND t2.newbalanceOrig < (t1.amount * 0.1)`  
`SET b.drain_flag = 1`  

I queried after this, and 62 results do show with drainflag = 1