Register 'lib.py' using org.apache.pig.scripting.jython.JythonScriptEngine as myfuncs;


results = LOAD 'input/results.txt' USING PigStorage(',') 
    AS (queryString:chararray, url:chararray, position:int);
revenue = LOAD 'input/revenue.txt' USING PigStorage(',') 
    AS (queryString:chararray, adSlot:chararray, amount:int);

grouped_data = COGROUP results BY queryString, revenue BY queryString;
STORE grouped_data into 'cogroup_out';

grouped_revenue = GROUP revenue BY queryString;
url_revenues = FOREACH grouped_data GENERATE
    FLATTEN(myfuncs.distributeRevenue(results, revenue));
STORE url_revenues into 'example3_out';

query_revenues = FOREACH grouped_revenue GENERATE
    group,
    SUM(revenue.amount) AS totalRevenue;
STORE query_revenues into 'group_out';

join_result = JOIN results BY queryString, revenue BY queryString;
STORE join_result into 'join_out';

grouped_revenue = GROUP revenue BY queryString;
query_revenues = FOREACH grouped_revenue{
    top_slot = FILTER revenue BY adSlot eq 'top';
    GENERATE group,
    SUM(top_slot.amount),
    SUM(revenue.amount);
};
STORE query_revenues into 'nested_out';
