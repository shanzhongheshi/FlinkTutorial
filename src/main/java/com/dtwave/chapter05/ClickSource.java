package com.dtwave.chapter05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import javax.lang.model.element.VariableElement;
import java.util.Calendar;
import java.util.Random;

/**
 * @author Lin
 * 2022/5/1  21:25
 */
public class ClickSource implements SourceFunction<Event> {
    //声明一个布尔变量，作为控制数据生成的标识位
    private Boolean running =true;

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (running) {
            sourceContext.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            //隔1秒生成一个点击时间，方便观测
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
