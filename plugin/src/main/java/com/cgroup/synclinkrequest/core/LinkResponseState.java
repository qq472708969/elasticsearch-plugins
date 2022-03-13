package com.cgroup.synclinkrequest.core;

/**
 * Created by zzq on 2022/3/11.
 */
public enum LinkResponseState {
    //重复执行该算子
    Repeat(10000),
    //请求处理成功，继续执行流程
    Success(1),
    //请求处理失败，终止流程
    Fail(0),
    //请求时出现异常，终止流程
    Exception(-1),
    //无后续，终止流程
    None(-2);

    LinkResponseState(int state) {
        this.state = state;
    }

    int state;

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }
}
