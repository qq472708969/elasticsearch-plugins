package com.cgroup.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by zzq on 2022/4/1/001.
 * <p>
 * 这里注意下，如果没有keyBy后的开窗操作则使用key打散二次聚合是会有重复累加问题；
 * 所以使用key打散，必须有开窗才行，否则使用本地预聚合或者自定义partitioner
 */
public class Times2Agg {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("taskmanager.numberOfTaskSlots", "4");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);

        env.disableOperatorChaining();//禁止槽共享

        SingleOutputStreamOperator<String> sourceStream = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                String keyTop1 = "abc";
                int count = 0;
                for (; ; ) {
                    if (count < 20) {
                        sourceContext.collect(keyTop1);
                    } else {
                        sourceContext.collect(count + keyTop1);
                        count = 0;
                    }
                    count++;
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {

            }
        }).name("source");

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapToT2Stream = sourceStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                int l = Long.hashCode(System.currentTimeMillis());
                int index = l & 10;
                return Tuple2.of(s + "_" + index, 1);
            }
        }).name("map-to-t2");

        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = mapToT2Stream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });

        SingleOutputStreamOperator<Tuple3<String, Integer, String>> reduceStream = tuple2StringKeyedStream
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                                return Tuple2.of(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
                            }
                        },
                        new ProcessWindowFunction<Tuple2<String, Integer>, Tuple3<String, Integer, String>, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<Tuple2<String, Integer>, Tuple3<String, Integer, String>, String, TimeWindow>.Context context
                                    , Iterable<Tuple2<String, Integer>> elements, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                                Tuple2<String, Integer> next = elements.iterator().next();
                                //t-3记录了窗口的结束时间，后面进行二次数据合并
                                out.collect(Tuple3.of(next.f0, next.f1, context.window().getEnd() + ""));
                            }
                        }
                ).name("1-reduce");

        SingleOutputStreamOperator<Tuple3<String, Integer, String>> reduce2Stream = reduceStream
                .map(new MapFunction<Tuple3<String, Integer, String>, Tuple3<String, Integer, String>>() {
                    @Override
                    public Tuple3<String, Integer, String> map(Tuple3<String, Integer, String> value) throws Exception {
                        String key = value.f0.split("\\_")[0];
                        return Tuple3.of(key, value.f1, value.f2);
                    }
                })
                .keyBy(new KeySelector<Tuple3<String, Integer, String>, String>() {
                    @Override
                    public String getKey(Tuple3<String, Integer, String> value) throws Exception {
                        return value.f0 + value.f2;
                    }
                })
                .reduce(new ReduceFunction<Tuple3<String, Integer, String>>() {
                    @Override
                    public Tuple3<String, Integer, String> reduce(Tuple3<String, Integer, String> value1, Tuple3<String, Integer, String> value2) throws Exception {
                        return Tuple3.of(value1.f0, value1.f1 + value2.f1, value1.f2);
                    }
                }).name("2-reduce");

        reduce2Stream.print();


        env.execute("win-jishu");
    }


    /**
     * 以下是错误示例
     *
     * @param args
     * @throws Exception
     */
    public static void main1(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("taskmanager.numberOfTaskSlots", "4");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);

        env.disableOperatorChaining();//禁止槽共享


        SingleOutputStreamOperator<String> sourceStream = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                String keyTop1 = "abc";
                int count = 0;
                for (; ; ) {
                    if (count < 20) {
                        sourceContext.collect(keyTop1);
                    } else {
                        sourceContext.collect(count + keyTop1);
                        count = 0;
                    }
                    count++;
                    Thread.sleep(1500L);
                }
            }

            @Override
            public void cancel() {

            }
        }).name("source");

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapToT2Stream = sourceStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                Random r = new Random(1);
                int i = r.nextInt(100);
                return Tuple2.of(s + "_" + i, 1);
            }
        }).name("map-to-t2");


        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = mapToT2Stream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = tuple2StringKeyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                return Tuple2.of(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
            }
        }).name("1-reduce");

        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream1 = reduceStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0.split("\\_")[0];
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> name = tuple2StringKeyedStream1.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                return Tuple2.of(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
            }
        }).name("2-reduce");

        name.print();

        env.execute("jishu");

    }
}
