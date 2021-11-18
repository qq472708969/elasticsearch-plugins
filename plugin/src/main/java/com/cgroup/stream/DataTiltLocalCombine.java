package com.cgroup.stream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by zzq on 2021/11/18.
 */
public class DataTiltLocalCombine {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("taskmanager.numberOfTaskSlots", "24");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        System.out.println(Runtime.getRuntime().availableProcessors());
        env.setParallelism(6);
        //使用时间触发
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //开启checkpoint机制，1000毫秒为发送barrier的间隔时长，
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        //保证两次checkpoint操作的最小间隔为500毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);
        //任务被取消时，保留下state
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //超时时间5秒
        env.getCheckpointConfig().setCheckpointTimeout(5000L);
        //如果state执行checkpoint失败，则直接任务退出
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);
        //恢复（重试5次， 重启之间的延时时间10）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.of(3, TimeUnit.SECONDS)));


        SingleOutputStreamOperator<String> source1 = env.addSource(new ParallelSourceFunction<String>() {
            boolean loop = true;
            String key = ",zzq5";
            int a = 1;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (; a < 10; ) {
                    a++;
                    long mill = System.currentTimeMillis();
                    String s = mill + key;

                    ctx.collect(s);
                    ctx.emitWatermark(new Watermark(mill));

                    if (s.contains("5,zzq")) {
                        key = ",zzq5";
                        Thread.sleep(200L);
                        continue;
                    }
                    if (s.contains("3,zzq")) {
                        key = ",zzq5";
                        Thread.sleep(200L);
                        continue;
                    }

                    if (s.contains("6,zzq")) {
                        key = ",zzq7";
                        Thread.sleep(200L);
                        continue;
                    }
                    if (s.contains("8,zzq")) {
                        key = ",zzq10";
                        Thread.sleep(200L);
                        continue;
                    }
                    Thread.sleep(200L);
                    continue;
                }
            }

            @Override
            public void cancel() {
                loop = false;
            }
        }).uid("addSource1").name("addSource1").setParallelism(2);

        source1.print();

        source1.flatMap(new LocalCombineRichFlatMapFunction<String, Tuple2<String, Integer>>(10) {
            @Override
            public String getKey(String value) {
                return value.split("\\,")[1];
            }

            @Override
            public Tuple2<String, Integer> getOut(String value) {
                return Tuple2.of(value.split("\\,")[1], 1);
            }

            @Override
            public Tuple2<String, Integer> processOutValue0(Tuple2<String, Integer> currValue, Tuple2<String, Integer> calcValue) {
                return Tuple2.of(currValue.f0, currValue.f1 + calcValue.f1);
            }
        }).setParallelism(2).print();


//        source1.keyBy(new KeySelector<String, String>() {
//            @Override
//            public String getKey(String value) throws Exception {
//                return value.split("\\,")[1];
//            }
//        }).print();


        env.execute();


    }
}
