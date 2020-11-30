package com.yr.kafkastreaming;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[],byte[]> {

    public static final String PREFIX_MSG = "abc";
    //上下文信息
    private ProcessorContext context;

    public void init(ProcessorContext context){
        this.context = context;
    }

    //用于处理数据
    public void process(byte[] key,byte[] value){
        String ratingvalue = new String(value);
        if(ratingvalue.contains(PREFIX_MSG)){
            String bValue = ratingvalue.split(PREFIX_MSG)[1];
            context.forward("log".getBytes(),bValue.getBytes());
        }
    }

    public void punctuate(long timestamp){

    }

    public void close(){

    }

}
