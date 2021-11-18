package com.cgroup.stream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zzq on 2021/11/18.
 */
public abstract class LocalCombineRichFlatMapFunction<IN, OUT> extends RichFlatMapFunction<IN, OUT> implements CheckpointedFunction {

    ListState<Object> localCombineLs;

    Map<String, Tuple2<String, OUT>> countMap;

    AtomicInteger countAi;

    int batchSize = 100;

    public LocalCombineRichFlatMapFunction(int batchSize) {
        this.batchSize = batchSize;
    }

    public LocalCombineRichFlatMapFunction() {
    }

    @Override
    public void flatMap(IN value, Collector<OUT> out) throws Exception {
        String key = getKey(value);
        OUT currOut = getOut(value);
        Tuple2<String, OUT> tuple2 = countMap.get(key);
        put(key, tuple2, currOut);
        if (countAi.incrementAndGet() <= batchSize) {
            return;
        }
        for (String itemKey : countMap.keySet()) {
            out.collect(countMap.get(itemKey).f1);
        }
        countMap.clear();
        countAi.set(0);
    }

    /**
     * 获取数据的唯一标记
     *
     * @param value
     * @return
     */
    public abstract String getKey(IN value);

    /**
     * 获取数据的唯一标记
     *
     * @param value
     * @return
     */
    public abstract OUT getOut(IN value);

    /**
     * 当前值，和计算值的汇总计算
     *
     * @param currValue
     * @param calcValue
     * @return
     */
    public abstract OUT processOutValue0(OUT currValue, OUT calcValue);

    /**
     * 生成状态快照
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //清理原始状态
        localCombineLs.clear();
        if (countMap == null) {
            return;
        }
        //将当前最新数据加入到状态中
        for (String itemKey : countMap.keySet()) {
            localCombineLs.add(countMap.get(itemKey));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();
        localCombineLs = operatorStateStore.getListState(new ListStateDescriptor("localCombineLsd", Object.class));
        countAi = new AtomicInteger(0);
        countMap = new HashMap();
        //故障状态恢复计数Map
        if (!context.isRestored()) {
            return;
        }
        Iterable<Tuple2<String, OUT>> tuple2s = (Iterable) localCombineLs.get();
        for (Tuple2<String, OUT> tuple2 : tuple2s) {
            Tuple2<String, OUT> tuple2CalcValue = countMap.get(tuple2.f0);
            put(tuple2.f0, tuple2CalcValue, tuple2.f1);
        }
    }

    public void put(String key, Tuple2<String, OUT> tuple2CalcOut, OUT currOut) {
        Tuple2<String, OUT> newTuple2 = Tuple2.of(key
                , tuple2CalcOut == null ? currOut : processOutValue0(currOut, tuple2CalcOut.f1));
        countMap.put(key, newTuple2);
    }
}
