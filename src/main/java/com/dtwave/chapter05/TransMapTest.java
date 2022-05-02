package com.dtwave.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Lin
 * 2022/5/1  22:01
 */
public class TransMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 2000L),
                new Event("Bob", "./cart", 3000L)
        );
        //传入匿名类，实现MapFunction
//        stream.map(new MapFunction<Event, String>() {
//            public String map(Event event) throws Exception {
//                return event.user;
//            }
//        }).print();


        //传入MapFunction的实现类
        stream.map(new UserExtractor()).print();
        System.out.println("Flink算子测试");
        env.execute();

    }

    public static class UserExtractor implements MapFunction<Event, String> {
        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
