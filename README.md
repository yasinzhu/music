# 音乐大数据离线及实时项目
本人业余时间完成,集合Sqoop,Flume,Hive,HDFS,MySQL,Spark,Redis,Kafka,SpringBoot技术实现.
- 离线部分:  
  离线数据通过Sqoop采集业务数据库数据,Flume采集日志数据,落盘到HDFS中,使用Hive进行数据关联建表,
  后期使用SparkCore进行数据清洗,SparkSQL做业务分析,然后入库到MySQL中使用Superset报表进行展示.
  主要实现功能:机器分布区域统计,日活量统计,商圈收入分成统计,订单数据统计,歌手歌曲点播排名统计等.
- 实时部分:  
  实时日志数据通过Flume采集,然后传输到Kafka中,使用SparkStreaming进行业务分析,然后入库到MySQL
  中使用Superset报表进行展示.
  主要实现功能:实时PV/UV统计,实时热度歌曲Top10等.