package com.cgroup.synclinkrequest;

/**
 * Created by zzq on 2022/3/11.
 */
public class LinkResponse {
    private LinkResponseState state;
    private String data;
    private String msg;

    public LinkResponseState getState() {
        return state;
    }

    public void setState(LinkResponseState state) {
        this.state = state;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
