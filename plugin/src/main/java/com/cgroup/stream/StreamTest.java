package com.cgroup.stream;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * Created by zzq on 2021/10/22.
 */
public class StreamTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        //保证两次checkpoint操作间隔为500毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);
        //任务被取消时，保留下state
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //超时时间5秒
        env.getCheckpointConfig().setCheckpointTimeout(5000L);
        //如果state执行checkpoint失败，则直接任务退出
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);

        DataStreamSource<String> stringDataStreamSource = env.addSource(new SourceFunction<String>() {
            boolean loop = true;
            String key = ",zzq";

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (; loop; ) {
                    String s = System.currentTimeMillis() + key;

                    ctx.collect(s);

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
        });

        DataStream<String> stringDataStream = stringDataStreamSource.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String s, int i) {
                if (s.endsWith("zzq8")) {
                    return 0;
                } else if (s.endsWith("zzq5")) {
                    return 11;
                } else {
                    return 2;
                }

            }
        }, new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s.split("\\,")[1];
            }
        });

        SingleOutputStreamOperator<String> countVS = stringDataStream.process(new StateProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                super.processElement(value, ctx, out);
            }
        });


        countVS.print();

        env.execute();
    }
}
