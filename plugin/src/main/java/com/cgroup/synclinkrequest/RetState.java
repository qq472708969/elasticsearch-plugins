package com.cgroup.synclinkrequest;

/**
 * Created by zzq on 2022/3/11.
 */
public enum RetState {
    Success(1), Fail(0);

    RetState(int state) {
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
