# Bounding box queries on point data stored in Databricks SQL

## Instructions
- Run the code in the notebook '00 - Generate data and register geo functions' in order to create the necessary datasets and register the SQL functions that underpin the example query strategies.
- Look at the code in notebooks '01' and '02'. They describe how the strategies of:
  1. simple range-based where clause queries; and
  2. queries over a spatial grid index with multiple resolution levels
  
can be applied in a SQL environment.
- Run the example queries in each notebook on your SQL warehouse to get a sense of the performance characteristics of each. Detailed query execution statistics can be found in the 'Query History' section of the DBSQL workspace.
- Look at the 'data/01 - Taxi data' notebook to see how a Delta table can be 'z-ordered' based on the lats / lons of each point in your dataset.

## Work outstanding
- Determine why data skipping is not kicking in when using 'Strategy 2'.
- Tune the distance factor parameter for selecting the optimal H3 resolution.
- Repeat tests across the different test geometries.
- Benchmarking test harness (intial workings are in the `/scratch` folder).