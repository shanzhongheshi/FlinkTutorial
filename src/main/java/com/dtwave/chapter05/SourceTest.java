package com.dtwave.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author Lin
 * 2022/5/1  19:10
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.csv");

        //2.从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<Integer>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);


        ArrayList<Event> events = new ArrayList<Event>();
        //这边的时间是毫秒
        events.add(new Event("Mary", "./home", 2000L));
        events.add(new Event("Bob", "./cart",3000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        //3.从元素中读取数据
        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("Mary", "./home", 2000L),
                new Event("Bob", "./cart", 3000L)
        );

        //4.从Socket文本流读取数据
        DataStreamSource<String> stream4 = env.socketTextStream("localhost", 7777);
//        stream1.print("1");
//        numStream.print("nums");
//        stream2.print("2");
        stream3.print("3");
        stream4.print("4");
        env.execute();

    }
}
