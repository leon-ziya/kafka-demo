#!/bin/bash
# 1. 判断没有参数的时，输出提示
if [ $# == 0 ];then
  echo -e "请输入start     启动kafka集群；\n请输入stop     关闭kafka集群；"&&exit
fi

# 2. 执行命令
case $1 in
  "start")
    for host in hadoop103 hadoop102 hadoop104
      do
        echo "========== 在$host启动kafka =========="
        ssh $host "kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties"
      done
  ;;
  "stop")
    for host in hadoop103 hadoop102 hadoop104
      do
        echo "========== 在$host关闭kafka =========="
        ssh $host "kafka-server-stop.sh "
      done
  ;;
  *)
    echo -e "请输入start     启动kafka集群；\n请输入stop     关闭kafka集群；"
  ;;
esac
