#!/bin/bash

USER=${1:-`whoami`}
NODES=(khaki07 khaki06 khaki05)
MASTER=${NODES[0]}

HADOOP_DIR=~/hadoop
export HADOOP_HOME=${HADOOP_DIR}/hadoop-2.6.0

generate_ssh_key() {
    if [ ! -f ~/.ssh/id_rsa.pub ]; then
        ssh-keygen
    fi
}

copy_ssh_key() {
    for node in "${NODES[@]}"; do
        machine="${USER}@${node}"
        ssh-copy-id -i ~/.ssh/id_rsa.pub "${machine}"
    done
}

install_hadoop() {
    rm -f hadoop-2.6.0.tar.gz
    rm -rf ${HADOOP_DIR}
    wget ftp://ftp.task.gda.pl/pub/www/apache/dist/hadoop/core/hadoop-2.6.0/hadoop-2.6.0.tar.gz
    mkdir -p ${HADOOP_DIR}
    tar -xvf hadoop-2.6.0.tar.gz -C ${HADOOP_DIR} &> /dev/null
    rm -f hadoop-2.6.0.tar.gz
}

setup_hadoop() {
    cat <<EOF > ${HADOOP_HOME}/etc/hadoop/core-site.xml
    <configuration>
        <property>
            <name>fs.defaultFS</name>
            <value>hdfs://${MASTER}:9000</value>
        </property>
    </configuration>
EOF

    cat <<EOF > ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml
    <configuration>
        <property>
            <name>dfs.replication</name>
            <value>1</value>
            </property>
    </configuration>
EOF
    echo -n "" >  ${HADOOP_HOME}/etc/hadoop/slaves
    for node in "${NODES[@]}"; do
        echo ${node} >> ${HADOOP_HOME}/etc/hadoop/slaves
    done
    sed -i -e "s|^export JAVA_HOME=\${JAVA_HOME}|export JAVA_HOME=${JAVA_HOME}|g" ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh
}

start_hadoop() {
    current_dir=$(pwd)
    cd ${HADOOP_HOME}
    bin/hdfs namenode -format
    sbin/start-dfs.sh
    bin/hdfs dfs -mkdir /user
    bin/hdfs dfs -mkdir /user/${USER}
    cd ${current_dir}   
}

generate_ssh_key
copy_ssh_key
install_hadoop
setup_hadoop
start_hadoop
