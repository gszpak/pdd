queries = LOAD 'input/query_log.txt' USING PigStorage(',') AS (userId, queryString, timestamp);
real_queries = FILTER queries BY userId neq 'bot';
STORE real_queries INTO 'filter_out';
