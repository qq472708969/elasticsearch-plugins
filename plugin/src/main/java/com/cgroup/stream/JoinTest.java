package com.cgroup.stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

/**
 * Created by zzq on 2021/10/26.
 */
public class JoinTest {
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

        SingleOutputStreamOperator<String> stringDataStreamSource2 = env.addSource(new SourceFunction<String>() {
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
        }).uid("addSource2");

        KeyedStream<String, String> stringStringKeyedStream1 = stringDataStreamSource1.keyBy(in -> in.split("\\,")[1]);

        KeyedStream<String, String> stringStringKeyedStream2 = stringDataStreamSource2.keyBy(in -> in.split("\\,")[1]);

        ConnectedStreams<String, String> connect = stringStringKeyedStream1.connect(stringStringKeyedStream2);

        SingleOutputStreamOperator<String> pe1 = connect.process(new KeyedCoProcessFunction<String, String, String, String>() {
            public MapState<String, String> kkv = null;


            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                StateTtlConfig stateTtlConfig =
                        StateTtlConfig.newBuilder(Time.seconds(1L))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                MapStateDescriptor<String, String> pe1 = new MapStateDescriptor<>("pe1", String.class, String.class);
                pe1.enableTimeToLive(stateTtlConfig);
                kkv = getRuntimeContext().getMapState(pe1);
            }

            @Override
            public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
                String key = value.split("\\,")[1];
                kkv.put(key, value);
            }

            @Override
            public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                String key = value.split("\\,")[1];
                String s = kkv.get(key);
                kkv.remove(key);
                if (StringUtils.isNotBlank(s)) {

                    out.collect("输出了：>" + s);
                }
            }
        });

        pe1.print();

        env.getExecutionPlan();

        env.execute();
    }
}
