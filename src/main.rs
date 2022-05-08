use ballista::prelude::*;
use datafusion::arrow::util::pretty;
use datafusion::prelude::CsvReadOptions;

#[tokio::main]
async fn main() -> Result<()> {
   // create configuration
   let config = BallistaConfig::builder()
       .set("ballista.shuffle.partitions", "4")
       .build()?;

   // connect to Ballista scheduler
   let ctx = BallistaContext::remote("localhost", 50050, &config).await?;

   // register csv file with the execution context
   ctx.register_csv(
       "tripdata",
       "/path/to/yellow_tripdata_2020-01.csv",
       CsvReadOptions::new(),
   ).await?;

   // execute the query
   let df = ctx.sql(
       "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount), AVG(fare_amount), SUM(fare_amount)
       FROM tripdata
       GROUP BY passenger_count
       ORDER BY passenger_count",
   ).await?;

   // collect the results and print them to stdout
   let results = df.collect().await?;
   pretty::print_batches(&results)?;
   Ok(())
}
