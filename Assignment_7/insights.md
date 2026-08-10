Insights - Superstore Sales Analysis with Spark

Dataset
- Sample - Superstore.csv, 9,994 order line items, 21 columns.
- Already fairly clean going in (no duplicate rows, no missing values in
  any column), but the cleaning step is still built to handle duplicates/
  nulls defensively rather than assume that will always hold - and it did
  catch one real formatting issue worth noting below.

Data cleaning
- Column names were switched from "Order ID" / "Sub-Category" style to
  snake_case (order_id, sub_category, ...) so they're easier to reference
  in code.
- order_date and ship_date were text ("11/8/2016") and got parsed into
  real date columns.
- postal_code was cast to a string, since it's an identifier, not a
  number that should ever be summed or averaged.
- One real gotcha: a product name field contains an escaped quote mark
  (14 7/8"" x 11"" - literal inch marks), and Spark's CSV reader needed
  escape='"' and multiLine=True set explicitly, otherwise it silently
  mis-parsed that row and shifted every column after it by one. Worth
  remembering for any CSV with quotes inside quoted fields.

Filtering
- Filtering to quantity 2-10, Category=Furniture, Region=West brought the
  9,994 rows down to 632 - shows how quickly stacking a few conditions
  narrows a dataset.

Aggregation
- Across all 9,994 line items: total sales ~2.30M, average sale ~230,
  ranging from about $0.44 to $22,638.

Grouping
- Only Technology (~145K total profit) and Office Supplies (~122K) cleared
  the 100,000 profit bar - Furniture did not, despite Furniture's total
  sales (~742K) being close to Office Supplies' (~719K). Furniture makes
  almost as much in sales as Office Supplies but only about 18K in profit
  - a much thinner margin, and worth a closer look if this were a real
  business question (likely driven by heavier average discounts on
  furniture, since bulky/large items often get discounted more).

Transformations
- Narrow transformations (filter, select, withColumn) don't need to move
  data between partitions, so they're cheap.
- Wide transformations (groupBy, orderBy) require a shuffle - Spark has to
  redistribute rows across partitions so matching keys land together.
  The groupBy-by-category step is the most expensive part of this
  pipeline for that reason.
