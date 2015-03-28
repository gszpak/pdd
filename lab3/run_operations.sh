#!/bin/bash

copy_testfiles() {
    $HADOOP_PREFIX/bin/hdfs dfs -mkdir /rel_algebra
    for FILE in testfiles/*
    do
        $HADOOP_PREFIX/bin/hdfs dfs -put $FILE /rel_algebra
    done
}

run_operations() {
    $HADOOP_PREFIX/bin/hadoop jar MRRelationalAlgebra-1.0.jar selection /rel_algebra/proj_sel_input /rel_algebra/sel_output "4#>=#3000" &&
    $HADOOP_PREFIX/bin/hadoop jar MRRelationalAlgebra-1.0.jar projection /rel_algebra/proj_sel_input /rel_algebra/proj_output "1#2#4" &&
    $HADOOP_PREFIX/bin/hadoop jar MRRelationalAlgebra-1.0.jar union /rel_algebra/un_int_diff_input1 /rel_algebra/un_int_diff_input2 /rel_algebra/un_output &&
    $HADOOP_PREFIX/bin/hadoop jar MRRelationalAlgebra-1.0.jar intersection /rel_algebra/un_int_diff_input1 /rel_algebra/un_int_diff_input2 /rel_algebra/int_output &&
    $HADOOP_PREFIX/bin/hadoop jar MRRelationalAlgebra-1.0.jar difference /rel_algebra/un_int_diff_input1 /rel_algebra/un_int_diff_input2 /rel_algebra/diff_output &&
    $HADOOP_PREFIX/bin/hadoop jar MRRelationalAlgebra-1.0.jar join /rel_algebra/join_input1 /rel_algebra/join_input2 /rel_algebra/join_output &&
    $HADOOP_PREFIX/bin/hadoop jar MRRelationalAlgebra-1.0.jar group /rel_algebra/group_input /rel_algebra/group_output
}

echo "Copying testfiles"
copy_testfiles
echo "Running operations"
run_operations
