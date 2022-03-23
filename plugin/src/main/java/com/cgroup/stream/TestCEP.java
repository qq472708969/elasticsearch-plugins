package com.cgroup.stream;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.events.Event;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by zzq on 2022/3/23.
 */
public class TestCEP {
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


        SingleOutputStreamOperator<String> stringDataStreamSource = env.addSource(new SourceFunction<String>() {

            String ph = "13687980889";
            String state0 = "success";
            String state1 = "fail";
            boolean ret = true;
            int count = 0;
            int error = 0;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {

                for (; ret; ) {
                    if (count > 10) {
                        sourceContext.collect(ph + "," + state1);
                        error++;
                        if (error > 4) {
                            count = 0;
                            error = 0;
                        }
                        Thread.sleep(599L);
                        continue;
                    }
                    sourceContext.collect(ph + "," + state0);
                    count++;


                    Thread.sleep(599L);
                }
            }

            @Override
            public void cancel() {
                ret = false;
            }
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(1L)) {
                    @Override
                    public long extractTimestamp(String s) {
                        return System.currentTimeMillis();
                    }
                });

        Pattern<String, String> patternLogin = Pattern.

                <String>begin("第一次").where(new SimpleCondition<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.split("\\,")[1].contains("fail");
            }
        })
                .next("第二次").where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.split("\\,")[1].contains("fail");
                    }
                })

                .next("第三次").where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.split("\\,")[1].contains("fail");
                    }
                }).within(Time.seconds(2));

        KeyedStream<String, String> stringStringKeyedStream = stringDataStreamSource
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value.split("\\,")[0];
                    }
                });

        PatternStream<String> patternStream = CEP.pattern(stringStringKeyedStream
                , patternLogin);

        SingleOutputStreamOperator<String> select = patternStream.select(new PatternSelectFunction<String, String>() {
            @Override
            public String select(Map<String, List<String>> pattern) throws Exception {
                return "1：" + pattern.get("第一次").get(0) +
                        " 2：" + pattern.get("第二次").get(0) +
                        " 3：" + pattern.get("第三次").get(0)
                        ;
            }
        });
        //stringStringKeyedStream.print();
        select.print();

        env.execute();
    }
}
