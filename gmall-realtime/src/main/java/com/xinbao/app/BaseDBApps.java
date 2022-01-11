package com.xinbao.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.function.CustomerDeserialization;
import com.xinbao.utils.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BaseDBApps {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink/ck"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2,2000L));

        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app_20210325";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        //TODO 将每行数据转换为JSON对象并进行过滤(delete)主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String type = jsonObject.getString("type");
                return !"delete".equals(type);
            }
        });
        //TODO 使用flink cdc 消费配置表并处理成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306).username("root").password("111111")
                .databaseList("gmall-20210325-realtime")
                .tableList("gmall-210325-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization()).build();

        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);



        env.execute();
    }
}
