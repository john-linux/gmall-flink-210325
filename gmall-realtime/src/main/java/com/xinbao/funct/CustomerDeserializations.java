package com.xinbao.funct;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

public class CustomerDeserializations implements DebeziumDeserializationSchema<String> {


    /**
     * 封装的数据格式
     * {
     * "database":"",
     * "tableName":"",
     * "before":{"id":"","tm_name":""....},
     * "after":{"id":"","tm_name":""....},
     * "type":"c u d",
     * //"ts":156456135615
     * }
     */

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        // 1.创建json对象用于存储最终数据
        JSONObject result = new JSONObject();
        // 2.获取库名 表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];



    }

    @Override
    public TypeInformation<String> getProducedType() {
        return null;
    }
}
