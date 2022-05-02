package com.dtwave.chapter05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import javax.lang.model.element.VariableElement;
import java.util.Random;

/**
 * 05
 *作用：自定义并行source
 * @author Lin
 * 2022/5/1  21:43
 */
public class SourceCustomParallelTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new CustomSource()).setParallelism(2).print();
        env.execute();
    }
    public static class CustomSource implements ParallelSourceFunction<Integer>{
        private boolean running =true;
        private Random random = new Random();
        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            while (running) {
                sourceContext.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {
            running=false;
        }
    }
}
