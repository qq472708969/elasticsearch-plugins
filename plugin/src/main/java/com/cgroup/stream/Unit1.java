package com.cgroup.stream;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Created by zzq on 2022/3/20/020.
 *
 * 触发器练习
 */
public class Unit1 {
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
        env.setStateBackend(new FsStateBackend("file:///D:/elasticsearch-plugins/plugin/src/main"));
        //恢复（重试2次， 重启之间的延时时间3）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, org.apache.flink.api.common.time.Time.of(5, TimeUnit.SECONDS)));
        /**
         * 每100毫秒标记一次Watermark
         */
        env.getConfig().setAutoWatermarkInterval(100L);

        SingleOutputStreamOperator<Tuple2<String, Integer>> stringDataStreamSource = env.socketTextStream("127.0.0.1", 9999).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        if ("null".equals(s)) {
                            throw new RuntimeException("sb");
                        }
                        String[] aryStr = s.split("\\,");
                        if (aryStr.length > 0) {
                            for (String item : aryStr) {
                                Tuple2<String, Integer> ret = Tuple2.of(item, 1);
                                collector.collect(ret);
                            }
                        }
                    }
                })
                //处理乱序数据  等1秒  ; window窗口触发时间步长为3(前包后不包)  当时间增加3毫秒时，触发（ “实际时间戳”-1秒） 的统计值。
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                        .withTimestampAssigner(new TimestampAssignerSupplier<Tuple2<String, Integer>>() {
                            @Override
                            public TimestampAssigner<Tuple2<String, Integer>> createTimestampAssigner(Context context) {
                                return new TimestampAssigner<Tuple2<String, Integer>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Integer> element, long recordTimestamp) {
                                        return Long.valueOf(element.f0.split("\\-")[1]);
                                    }
                                };
                            }
                        })

                );
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Integer>>(Time.seconds(1L)) {
//                    @Override
//                    public long extractTimestamp(Tuple2<String, Integer> element) {
//                        return Long.valueOf(element.f0.split("\\-")[1]);
//                    }
//                });
        OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<Tuple2<String, Integer>>("abc") {
        };

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = stringDataStreamSource.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0.split("\\-")[0];
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(3L)))
                //使用ProcessingTime自定义Trigger来处理EventTime在长时间无数据的情况下无法关窗问题
                // new ValueStateDescriptor<Long>("close", Long.class)
//                .trigger(new Trigger<Tuple2<String, Integer>, TimeWindow>() {
//
//                    @Override
//                    public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
//                        ValueState<Long> close = ctx.getPartitionedState(new ValueStateDescriptor<Long>("close", Long.class));
//                        Long closeTimer = close.value();
//                        long timestamp1 = System.currentTimeMillis();
//                        //记录当前的系统处理时间，用系统时间作为关窗都低逻辑
//                        if (closeTimer != null) {
//                            //如果已经有了兜底关窗数据，则更新定时器的触发时间，并重新记录
//                            ctx.deleteProcessingTimeTimer(closeTimer);
//                        }
//                        //10秒后触发关窗
//                        ctx.registerProcessingTimeTimer(timestamp1 + 10000);
//                        close.update(timestamp1 + 10000);
//                        return TriggerResult.CONTINUE;
//                    }
//
//                    @Override
//                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//                        return TriggerResult.FIRE_AND_PURGE;
//                    }
//
//                    @Override
//                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//                        return TriggerResult.FIRE_AND_PURGE;
//                    }
//
//                    @Override
//                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
//
//                    }
//                })
//                .allowedLateness(Time.seconds(3L))
//                .sideOutputLateData(outputTag)
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    public MapState<String, Integer> registrationState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        registrationState = getRuntimeContext().getMapState(new MapStateDescriptor("registrationState", String.class, Integer.class));
                    }

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        Integer integer = registrationState.get(s);
                        if (integer == null) {
                            registrationState.put(s, Iterables.size(iterable));
                        } else {
                            registrationState.put(s, registrationState.get(s) + Iterables.size(iterable));
                        }
                        collector.collect(Tuple2.of(s, registrationState.get(s)));
                    }


                });

        reduce.print();

        reduce.getSideOutput(outputTag);

        env.execute();

    }
}
