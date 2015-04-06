Register 'lib.py' using org.apache.pig.scripting.jython.JythonScriptEngine as myfuncs;


queries = LOAD 'input/query_log.txt' USING PigStorage(',')
    AS (userId:chararray, queryString:chararray, timestamp:double);

expanded_queries = FOREACH queries GENERATE
    userId, myfuncs.expandQuery(queryString);
STORE expanded_queries INTO 'expanded_out';

expanded_flattened = FOREACH queries GENERATE
    userId, FLATTEN(myfuncs.expandQuery(queryString));
STORE expanded_flattened into 'expanded_flattened_out';

real_queries = FILTER queries BY userId neq 'bot';
STORE real_queries INTO 'filter_out';
