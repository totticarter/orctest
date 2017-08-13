package com.tencent.cubeli.orc2;

/**
 * Created by waixingren on 8/13/17.
 */
public class ThreadRunInfo extends  Thread{

    public long getTime() {
        return time;
    }

    public int getSize() {
        return size;
    }

    private long time;
    private int size;
    public ThreadRunInfo(long time, long size){

    }

    public void run(){

    }
}
