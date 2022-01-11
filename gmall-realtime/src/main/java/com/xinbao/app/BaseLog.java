package com.xinbao.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xinbao.utils.KafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

public class BaseLog {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //TODO 2.消费 ods_base_log 主题数据创建流
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app_210325";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        OutputTag<String> outputTag = new OutputTag<String>("dirty"){};

        //TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    //发生异常,将数据写入侧输出流
                    ctx.output(outputTag, value);
                }
            }
        });
        //打印脏数据
        jsonObjDS.getSideOutput(outputTag).print("Dirty>>>>>>>>>>>");

        /**
         *
         * 算子状态数据结构
         * 键控状态数据结构
         *
         * 算子状态数据结构
         * ➢ 列表状态（List state） • 将状态表示为一组数据的列表
         *
         * ➢ 联合列表状态（Union list state） • 也将状态表示为数据的列表。它与常规列表状态的区别在于，在发生故
         * 障时，或者从保存点（savepoint）启动应用程序时如何恢复
         *
         * ➢ 广播状态（Broadcast state） • 如果一个算子有多项任务，而它的每项任务状态又都相同，那么这种特
         * 殊情况最适合应用广播状态。
         *
         * 键控状态数据结构
         * ➢ 值状态（Value state） • 将状态表示为单个的值
         * ➢ 列表状态（List state） • 将状态表示为一组数据的列表
         * ➢ 映射状态（Map state） • 将状态表示为一组 Key-Value 对
         * ➢ 聚合状态（Reducing state & Aggregating State） • 将状态表示为一个用于聚合操作的列表
         *
         * 选择一个状态后端
         * ➢ MemoryStateBackend
         * • 内存级的状态后端，会将键控状态作为内存中的对象进行管理，将它们存储在
         * TaskManager 的 JVM 堆上，而将 checkpoint 存储在 JobManager 的内存
         * 中 • 特点：快速、低延迟，但不稳定
         *
         * ➢ FsStateBackend
         * • 将 checkpoint 存到远程的持久化文件系统（FileSystem）上，而对于本地状
         * 态，跟 MemoryStateBackend 一样，也会存在 TaskManager 的 JVM 堆上
         * • 同时拥有内存级的本地访问速度，和更好的容错保证
         *
         * ➢ RocksDBStateBackend
         * • 将所有状态序列化后，存入本地的 RocksDB 中存储。
         *
         */

        /**
         * {"common":{"ar":"440000","ba":"Redmi","ch":"huawei","is_new":"0","md":"Redmi k30","mid":"mid_13","os":"Android 11.0","uid":"34","vc":"v2.1.134"},
         * "displays":[{"display_type":"query","item":"7","item_type":"sku_id","order":1,"pos_id":2},
         * {"display_type":"query","item":"4","item_type":"sku_id","order":2,"pos_id":1},
         * {"display_type":"promotion","item":"4","item_type":"sku_id","order":3,"pos_id":3},
         * {"display_type":"promotion","item":"4","item_type":"sku_id","order":4,"pos_id":1},
         * {"display_type":"recommend","item":"5","item_type":"sku_id","order":5,"pos_id":4},
         * {"display_type":"promotion","item":"7","item_type":"sku_id","order":6,"pos_id":3},
         * {"display_type":"query","item":"1","item_type":"sku_id","order":7,"pos_id":3}],
         * "page":{"during_time":5406,"item":"2","item_type":"sku_id","last_page_id":"good_detail","page_id":"good_spec","source_type":"promotion"},"ts":1641788500000}
         */
        //TODO 4.新老用户校验  状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    //定义状态 --值状态统计新老访客
                    private ValueState<String> valueState;
                    private SimpleDateFormat simpleDateFormat;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state",String.class));
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        //判断isNew标记是否为"1"
                        String isNew = value.getJSONObject("common").getString("is_new");
                        Long ts = value.getLong("ts");
                        if ("1".equals(isNew)){
                            //获取状态数据
                            String state = valueState.value();
                            if (state != null){
                                //修改isNew标记
                                value.getJSONObject("common").put("is_new","0");
                            }else {
                                //更新状态
                                valueState.update(simpleDateFormat.format(ts));
                            }
                        }
                        return value;
                    }
                });
        //TODO 5.分流  侧输出流  页面：主流  启动：侧输出流  曝光：侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {};
        OutputTag<String> displayTag = new OutputTag<String>("display") {};

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                String start = value.getString("start");
                if (start != null && start.length() > 0){
                    //将数据写入启动日志侧输出流
                    ctx.output(startTag,value.toJSONString());
                }else {
                    //将数据写入日志主流
                    out.collect(value.toJSONString());

                    //取出数据中的曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0){
                        //获取页面ID
                        String pageId = value.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            //添加页面Id
                            display.put("page_id",pageId);
                            //将输出写出到曝光侧输出流
                            ctx.output(displayTag,display.toJSONString());
                        }
                    }
                }
            }
        });

        //TODO 6.提取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);


        //TODO 7.将三个流进行打印并输出到对应的Kafka主题中
        startDS.print("Start>>>>>>>>>>>");
        pageDS.print("Page>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>>>>");

        startDS.addSink(KafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.addSink(KafkaUtil.getKafkaProducer("dwd_page_log"));
        displayDS.addSink(KafkaUtil.getKafkaProducer("dwd_display_log"));

        //TODO 8.启动任务
        env.execute("BaseLogApp");
    }
}
