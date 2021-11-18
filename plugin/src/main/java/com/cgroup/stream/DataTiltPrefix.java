package com.cgroup.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by zzq on 2021/11/16/016.
 *
 * 开窗聚合统计发生数据倾斜，可以使用随机后缀+时间窗口结束点+去后缀统计解决问题
 */
public class DataTiltPrefix {
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


        SingleOutputStreamOperator<String> stringDataStreamSource1 = env.addSource(new SourceFunction<String>() {
            boolean loop = true;
            String key = ",zzq5";
            int a = 1;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (; loop; ) {
                    a++;
                    long mill = System.currentTimeMillis();
                    String s = mill + key;

                    ctx.collect(s);
                    ctx.emitWatermark(new Watermark(mill - 200L));

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
        }).uid("addSource1").name("addSource1");

        //subtask职责增加随机后缀
        stringDataStreamSource1 = stringDataStreamSource1.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value + "_" + (int) (Math.random() * 10);
            }
        });

        stringDataStreamSource1.print();


        KeyedStream<String, String> stringStringKeyedStream = stringDataStreamSource1.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s.split("\\,")[1];
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> apply = stringStringKeyedStream.window(TumblingProcessingTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(6L))).apply(new WindowFunction<String, Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow timeWindow, Iterable<String> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                long end = timeWindow.getEnd();

                int size = Iterators.size(iterable.iterator());

                collector.collect(Tuple2.of(s + "_" + end, size));
            }
        });

        apply.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2;
            }
        }).print();


        //去掉随机数后，将相同的开窗数据合并计算，保证结果正确（如果不保留开窗end时间点，则无法保证开窗数据汇总的正确性）
        KeyedStream<Tuple2<String, Integer>, String> tuple2Tuple2KeyedStream = apply.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                String[] keyAry = value.f0.split("\\_");

                return keyAry[0] + keyAry[2];
            }
        });

        tuple2Tuple2KeyedStream.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                String[] keyAry = value.f0.split("\\_");
                return Tuple2.of(keyAry[0] + "_" + keyAry[2], value.f1);
            }
        }).print();


//        apply.

//        stringStringKeyedStream.map(new MapFunction<String, Tuple2<String, String>>() {
//            @Override
//            public Tuple2<String, String> map(String s) throws Exception {
//                String[] split = s.split("\\,");
//                return Tuple2.of(split[1], split[0]);
//            }
//        }).print();


        env.execute();
    }
}
