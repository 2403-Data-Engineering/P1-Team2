`MATCH (a)-[t1:TRANSACTION {type: 'TRANSFER'}]->(b)-[t2:TRANSACTION {type: 'CASH_OUT'}]->(c)`  
`WHERE t2.step > t1.step AND t2.step - t1.step <= 4 AND abs(t1.amount - t2.amount) / t1.amount < 0.5`  
`SET a.transfer_cashout_flag = 1, b.transfer_cashout_flag = 1, c.transfer_cashout_flag = 1`  

This query finds pairs of sequential transactions within 4 steps of each other, where the first transaction is a transfer, and the second transaction involved cashing out over 50% of the amount transferred. The fraud-reference doc specifies to flag both transactions, so I'm writing this to all accounts involved.  

I also went back and flagged all accounts involved in the drain signal. (There were 216 flagged accounts after changing this.)