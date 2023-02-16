package com.cyfqz;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountDataStream {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> localhost = env.socketTextStream("localhost", 2003);
//        SingleOutputStreamOperator<Object> map = localhost.map(new MapFunction<String, Tuple2<String,Integer>>() {
//            @Override
//            public Tuple2<String,Integer> map(String value) throws Exception {
//                String[] words = value.split(" ");
//                for (String word : words){
//                    return new Tuple2<>(word,1);
//                }
//            }
//        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapDM = localhost.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });



        KeyedStream<Tuple2<String, Integer>, String> keyByDS = flatMapDM.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyByDS.sum(1);

        sum.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
