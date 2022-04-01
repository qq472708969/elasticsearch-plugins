package com.cgroup.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Created by zzq on 2022/3/17.
 * <p>
 * 处理时间，使用定时器
 */
public class TimerTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("taskmanager.numberOfTaskSlots", "2");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(2);
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
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.of(3, TimeUnit.SECONDS)));

//        OutputTag<String> outputTag = new OutputTag("abc");

        DataStreamSource<String> stringDataStreamSource = env.addSource(new SourceFunction<String>() {

            String phoneNum = "13716055334";
            String state0 = "success";
            String state1 = "fail";
            boolean ret = true;
            int sum = 0;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {

                for (; ret; ) {
                    sum++;

                    if (sum < 20) {
                        System.out.println("》》》" + phoneNum + "," + state1);
                        sourceContext.collect(phoneNum + "," + state1);
                    } else {
                        System.out.println("》》》" + phoneNum + "," + state0);
                        sourceContext.collect(phoneNum + "," + state0);
                        if (sum > 50) {
                            sum = 0;
                        }
                    }


                    Thread.sleep(600L);
                }
            }

            @Override
            public void cancel() {
                ret = false;
            }
        });


        SingleOutputStreamOperator<String> mapRetStream = stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s.replace(",", "|");
            }
        });

        KeyedStream<String, String> stringStringKeyedStream = mapRetStream.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                String ret = s.split("\\|")[0];
                return ret;
            }
        });

        SingleOutputStreamOperator<String> registrationState = stringStringKeyedStream.process(new KeyedProcessFunction<String, String, String>() {
            public ValueState<Long> registrationState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                registrationState = getRuntimeContext().getState(
                        new ValueStateDescriptor("registrationState", Long.class));
            }

            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                String state = s.split("\\|")[1];
                //当前key为失败的数据状态，并且flinkState为false
                if ("fail".equals(state)) {
//                    context.output(outputTag, s);
                }

                if ("fail".equals(state) && Objects.isNull(registrationState.value())) {
                    //当前key发生的"处理时间戳"，增加6秒的定时器
                    long l = context.timerService().currentProcessingTime() + 6000;
                    //注册定时器
                    context.timerService().registerProcessingTimeTimer(l);
                    registrationState.update(l);
                }
                //如果当前flinkState状态为true，证明未触发报警； 并且在未触发报警前已经有正确状态返回，则直接删除定时器
                else if ("success".equals(state) && Objects.nonNull(registrationState.value())) {
                    //删除定时器
                    context.timerService().deleteProcessingTimeTimer(registrationState.value());
                    //重置状态
                    registrationState.clear();
                }

            }

            /**
             * 定时器
             * @param timestamp
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                String s = "报警啦，5秒没有成功，" + ctx.getCurrentKey();

                out.collect(s);
                registrationState.clear();
            }
        });

        registrationState.print();

//        registrationState.getSideOutput(outputTag).print("侧输出通道");


        env.execute("---ghghgh");
    }
}
