package com.cgroup.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zzq on 2021/11/8.
 *
 * 反压测试
 */
public class StringTestOnlyMap {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("taskmanager.numberOfTaskSlots", "20");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        env.setParallelism(6);
        //使用时间触发
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        //保证两次checkpoint操作间隔为500毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);
        //任务被取消时，保留下state
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //超时时间5秒
        env.getCheckpointConfig().setCheckpointTimeout(5000L);
        //如果state执行checkpoint失败，则直接任务退出
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);

        SingleOutputStreamOperator<String> stringDataStreamSource1 = env.addSource(new SourceFunction<String>() {
            boolean loop = true;
            String key = ",zzq";

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (; loop; ) {
                    long mill = System.currentTimeMillis();
                    String s = mill + key;

                    ctx.collect(s);
                    ctx.emitWatermark(new Watermark(mill - 2000L));

                    if (s.contains("5,zzq")) {
                        key = ",zzq5";
                        Thread.sleep(2000L);
                        continue;
                    }
                    if (s.contains("3,zzq")) {
                        key = ",zzq3";
                        Thread.sleep(1000L);
                        continue;
                    }

                    if (s.contains("6,zzq")) {
                        key = ",zzq6";
                        Thread.sleep(1000L);
                        continue;
                    }
                    if (s.contains("8,zzq")) {
                        key = ",zzq8";
                        Thread.sleep(1000L);
                        continue;
                    }
                    Thread.sleep(1000L);
                    continue;
                }
            }

            @Override
            public void cancel() {
                loop = false;
            }
        }).uid("addSource1");


        SingleOutputStreamOperator<String> listSingleOutputStreamOperator = stringDataStreamSource1.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                out.collect(value);
            }
        }).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String ret = value;
                Thread.sleep(3000L);
                return ret;
            }
        });

        listSingleOutputStreamOperator.print();


        env.execute();
    }
}
