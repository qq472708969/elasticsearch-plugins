package com.cgroup.stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Created by zzq on 2021/10/26.
 */
public class JoinEventTimeTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("taskmanager.numberOfTaskSlots", "24");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        System.out.println(Runtime.getRuntime().availableProcessors());
        env.setParallelism(6);
        //使用时间触发
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.of(10, TimeUnit.SECONDS)));

        //也可以根据失败率重启
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                // 每个测量时间间隔最大失败次数
                3,
                // 失败率测量的时间间隔（5分钟测量失败了3次则重启）
                Time.of(5,TimeUnit.MINUTES),
                // 两次连续重启尝试的时间间隔
                Time.of(10,TimeUnit.SECONDS)
        ));

        SingleOutputStreamOperator<String> stringDataStreamSource1 = env.addSource(new SourceFunction<String>() {
            boolean loop = true;
            String key = ",zzq5";
            int a = 1;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
//                for (; a==1; ) {
                for (; loop; ) {
                    a++;
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
        }).uid("addSource1").name("addSource1");

        SingleOutputStreamOperator<String> stringDataStreamSource2 = env.addSource(new SourceFunction<String>() {
            boolean loop = true;
            String key = ",zzq5";
            int a = 1;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
//                for (; a==1; ) {
                for (; loop; ) {
                    a++;
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
        }).uid("addSource2").name("addSource2");

        KeyedStream<String, String> stringStringKeyedStream = stringDataStreamSource2.slotSharingGroup("slot1").keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value.split("\\,")[1];
            }
        });

        stringStringKeyedStream.print().setParallelism(4).
                name("haha");

        KeyedStream<String, String> stringStringKeyedStream1 = stringDataStreamSource1
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(System.currentTimeMillis());
                    }

                    @Override
                    public long extractTimestamp(String element, long previousElementTimestamp) {
                        return Long.valueOf(element.split("\\,")[0]);
                    }
                }).keyBy(in -> in.split("\\,")[1]);

        KeyedStream<String, String> stringStringKeyedStream2 = stringDataStreamSource2
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(System.currentTimeMillis());
                    }

                    @Override
                    public long extractTimestamp(String element, long previousElementTimestamp) {
                        return Long.valueOf(element.split("\\,")[0]);
                    }
                }).keyBy(in -> in.split("\\,")[1]);

        ConnectedStreams<String, String> connect = stringStringKeyedStream1.connect(stringStringKeyedStream2);

        SingleOutputStreamOperator<String> pe1 = connect.process(new KeyedCoProcessFunction<String, String, String, String>() {
            public MapState<String, String> kkv = null;
            public ValueState<String> out = null;
            public ValueState<Long> vs = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig stateTtlConfig =
                        StateTtlConfig.newBuilder(Time.seconds(1L))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                MapStateDescriptor<String, String> pe1 = new MapStateDescriptor<>("pe1", String.class, String.class);
                pe1.enableTimeToLive(stateTtlConfig);
                kkv = getRuntimeContext().getMapState(pe1);
                vs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("vs", Long.class));
                out = getRuntimeContext().getState(new ValueStateDescriptor<String>("out", String.class));
            }

            @Override
            public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
                String key = value.split("\\,")[1];
                kkv.put(key, value);
            }

            @Override
            public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                if (vs.value() != null) {
                    ctx.timerService().deleteEventTimeTimer(vs.value());
                    vs.update(null);
                }

                String key = value.split("\\,")[1];
                String mills = value.split("\\,")[0];
                long l = Long.valueOf(mills) + 3L;
                //如果定时器已经被清除，则重新添加新的定时器
                String s = kkv.get(key);
                if (vs.value() == null && s != null) {
                    ctx.timerService().registerEventTimeTimer(l);
                    vs.update(l);
                }

                kkv.remove(key);
                if (StringUtils.isNotBlank(s)) {
                    this.out.update("输出了：>" + s);
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                if (timestamp == vs.value()) {
                    out.collect(this.out.value());
                    this.out.update("");
                }
            }
        });

        pe1.print();

        SingleOutputStreamOperator<Integer> ret = pe1.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return 1;
            }
        }).timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time.seconds(1L)).reduce((num1, num2) -> num1 + num2);

        ret.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer value) throws Exception {
                return value + "";
            }
        }).print();

//        Iterator<Integer> collect = DataStreamUtils.collect(ret);
//
//        Integer count = 0;
//        for (; collect.hasNext(); ) {
//            Integer next = collect.next();
//            count = count + next;
//            System.out.println("===>" + count);
//
//        }
//        env.getExecutionPlan();

        System.out.println(env.getExecutionPlan());


        env.execute();

    }
}
