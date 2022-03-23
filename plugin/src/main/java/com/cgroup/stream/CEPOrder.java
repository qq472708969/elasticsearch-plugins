package com.cgroup.stream;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by zzq on 2022/3/23.
 */
public class CEPOrder {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("taskmanager.numberOfTaskSlots", "24");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //开启checkpoint机制，1000毫秒为发送barrier的间隔时长，
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        //保证两次checkpoint操作的最小间隔为500毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //超时时间5秒
        env.getCheckpointConfig().setCheckpointTimeout(5000L);
        // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 如果有更近的保存点时，是否将作业回退到该检查点
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        //env.setStateBackend(new FsStateBackend("file:///D:/elasticsearch-plugins/plugin/src/main"));
        //恢复（重试2次， 重启之间的延时时间3）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, org.apache.flink.api.common.time.Time.of(5, TimeUnit.SECONDS)));
        /**
         * 每100毫秒标记一次Watermark
         */
        env.getConfig().setAutoWatermarkInterval(100L);

        //消息格式，订单+动作
        SingleOutputStreamOperator<String> socketTextStream = env.socketTextStream("127.0.0.1", 9999)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2L)) {
                    @Override
                    public long extractTimestamp(String s) {
                        return System.currentTimeMillis();
                    }
                });

        Pattern<String, String> orderProcessor = Pattern
                .<String>begin("created").where(new IterativeCondition<String>() {
                    @Override
                    public boolean filter(String value, Context<String> ctx) throws Exception {
                        String[] orderAry = value.split("\\+");
                        return orderAry[1].equalsIgnoreCase("created");
                    }
                })

                .followedBy("pay").where(new IterativeCondition<String>() {
                    @Override
                    public boolean filter(String value, Context<String> ctx) throws Exception {
                        String[] orderAry = value.split("\\+");
                        return orderAry[1].equalsIgnoreCase("pay");
                    }
                })
                .within(Time.seconds(10));

        PatternStream<String> orderStream = CEP.pattern(socketTextStream.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value.split("\\+")[0];
            }
        }), orderProcessor);

        OutputTag<String> timeoutOrder = new OutputTag("timeout-order") {
        };

        SingleOutputStreamOperator<String> select = orderStream.select(timeoutOrder
                , new PatternTimeoutFunction<String, String>() {
                    @Override
                    public String timeout(Map<String, List<String>> pattern, long timeoutTimestamp) throws Exception {
                        String payTimeout = pattern.get("created").get(0);
                        String[] orderAry = payTimeout.split("\\+");

                        return "orderId:" + orderAry[0] + " orderTimeout-stamp:" + timeoutTimestamp;
                    }
                }
                , new PatternSelectFunction<String, String>() {
                    @Override
                    public String select(Map<String, List<String>> pattern) throws Exception {
                        String paySuccess = pattern.get("pay").get(0);
                        String[] orderAry = paySuccess.split("\\+");
                        return "orderId:" + orderAry[0] + " order-state:" + orderAry[1];
                    }
                });

        select.print();
        select.getSideOutput(timeoutOrder).print();

        env.execute("order-process-complete");
    }
}
