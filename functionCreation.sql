DROP LIBRARY myavg CASCADE;
CREATE LIBRARY myavg AS '/home/vertica/vertica_myavg/myavg.so';
CREATE TRANSFORM FUNCTION myavg AS LANGUAGE 'C++' NAME 'SumEtCountFactory' LIBRARY myavg;
SELECT myavg(a) EVER (PARTITION BY id) FROM dataset;