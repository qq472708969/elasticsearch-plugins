package com.cgroup.stream;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Created by zzq on 2021/10/22.
 */
public abstract class StateProcessFunction<InputType, OutputType> extends ProcessFunction<InputType, OutputType> implements CheckpointedFunction {
    ListState<String> elementLS = null;
    int count = 0;

    @Override
    public void processElement(InputType value, Context ctx, Collector<OutputType> out) throws Exception {
        count++;
        System.out.println("=>>>" + value + "  =>>>" + count);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    /**
     * 初始化阶段
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListState<String> elementLS = context.getOperatorStateStore().getListState(new ListStateDescriptor<String>("elementLS", String.class));
        this.elementLS = elementLS;
        //重启标记，重启后将之前的元素总数同步加入
        if (context.isRestored()) {
            count = Iterables.size(elementLS.get());
        }
    }
}
