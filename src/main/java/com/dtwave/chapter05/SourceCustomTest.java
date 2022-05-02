package com.dtwave.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 04：自定义source实现
 * @author Lin
 * 2022/5/1  21:35
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);

        DataStreamSource stream = env.addSource(new ClickSource());
        stream.setParallelism(2).print("SourceCustom");
        //报错：Exception in thread "main" java.lang.IllegalArgumentException: The parallelism of non parallel operator must be 1.
        env.execute();
    }
}
