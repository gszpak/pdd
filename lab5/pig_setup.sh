#!/bin/bash 

PIG_DIR=~/pig

install_pig() {
    rm -f pig-0.14.0.tar.gz
    wget ftp://ftp.ps.pl/pub/apache/pig/latest/pig-0.14.0.tar.gz
    mkdir -p $PIG_DIR
    tar -xvf pig-0.14.0.tar.gz -C $PIG_DIR &> /dev/null
    rm -f pig-0.14.0.tar.gz
    export PATH=$PIG_DIR/pig-0.14.0/bin:$PATH
}

run_pig() {
    pig -x local example1.pig &&
    pig -x local example2.pig &&
    pig -x local example3.pig
}

install_pig &&
run_pig
