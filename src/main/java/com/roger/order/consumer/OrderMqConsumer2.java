package com.roger.order.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class OrderMqConsumer2 {

    public static void main(String[] args) throws Exception{
        DefaultMQPushConsumer defaultMQPushConsumer =
                new DefaultMQPushConsumer("orderMQPushConsumerGroup");
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        defaultMQPushConsumer.setNamesrvAddr("172.20.10.60:9876");

        defaultMQPushConsumer.subscribe("OrderTopic","*");

        defaultMQPushConsumer.registerMessageListener(new MessageListenerOrderly() {
            Random r = new Random();
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgExtList, ConsumeOrderlyContext context) {
                context.setAutoCommit(true);
                System.out.println("当前线程名：" + Thread.currentThread().getName() + ";Receive new message:");
                for(MessageExt msgExt : msgExtList){
                    System.out.println(String.format("Consume message [%s],TagName [%s]",
                            new String(msgExt.getBody()),
                            msgExt.getTags()));
                    try {
                        //简单业务处理逻辑
                        TimeUnit.SECONDS.sleep(r.nextInt(10));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        defaultMQPushConsumer.start();

        System.out.println("OrderMqConsumer2 Started...");
    }
}
