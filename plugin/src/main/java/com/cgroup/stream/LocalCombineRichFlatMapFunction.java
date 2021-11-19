package com.cgroup.stream;

import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zzq on 2021/11/18.
 */
public abstract class LocalCombineRichFlatMapFunction<IN, OUT> extends RichFlatMapFunction<IN, OUT> implements CheckpointedFunction {

    private ListState<Tuple2<String, OUT>> localCombineLs = null;

    private Map<String, Tuple2<String, OUT>> countMap;

    private AtomicInteger countAi;

    private int batchSize = 100;

    protected LocalCombineRichFlatMapFunction(int batchSize) {
        this.batchSize = batchSize;
    }

    protected LocalCombineRichFlatMapFunction() {
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
        for (Tuple2<String, OUT> tuple2Item : countMap.values()) {
            if (tuple2Item == null) {
                continue;
            }
            out.collect(tuple2Item.f1);
        }
        //发送到下一个subTask后，可清空当前的集合统计值，不用做状态保存
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
     * checkPoint状态定期生成
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (MapUtils.isEmpty(countMap)) {
            return;
        }
        //清理原始状态
        localCombineLs.clear();
        //将当前最新数据加入到状态中
        for (Tuple2<String, OUT> tuple2Item : countMap.values()) {
            localCombineLs.add(tuple2Item);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        Type currType = this.getClass().getGenericSuperclass();
        //强转为参数化类型实体
        ParameterizedType currParameterizedType = (ParameterizedType) currType;
        //获取具体实现泛型参数的列表
        Type[] currTypeAry = currParameterizedType.getActualTypeArguments();
        //获取OUT输出参数的实际类型
        Type outType = currTypeAry[1];
        localCombineLs = context.getOperatorStateStore()
                .getListState(new ListStateDescriptor<Tuple2<String, OUT>>("localCombineLsd"
                        , Types.TUPLE(Types.STRING, TypeInformation.of(outType.getClass()))));
        countAi = new AtomicInteger(0);
        countMap = new HashMap<>(batchSize * 3);
        //故障状态恢复计数Map
        if (!context.isRestored()) {
            return;
        }
        Iterable<Tuple2<String, OUT>> tuple2s = localCombineLs.get();
        if (Iterables.isEmpty(tuple2s)) {
            return;
        }
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
