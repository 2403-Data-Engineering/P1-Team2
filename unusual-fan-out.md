## Step 1: Write the outdegree property to every node using gds write

`CALL gds.graph.project('graph0','*','*')`  
(NOTE: This command has to be ran everytime you restart your instance.)  

`CALL gds.degree.write('graph0', {writeProperty: 'outdegree'})`  
`YIELD centralityDistribution`  
`RETURN centralityDistribution` 



## Step 2: Query the database to find outliers to be flagged.  
In this case, if an account sends money to more than 99.9% of other accounts, it is flagged.

`CALL gds.degree.stats('graph0')`  
`YIELD centralityDistribution`  
`MATCH (a:Account)-[t:TRANSACTION]->(b:Account)`  
`WHERE a.outdegree > centralityDistribution.p999`  
`RETURN t.step, a.id, a.outdegree`  
`ORDER BY t.step ASC, a.outdegree DESC`  
