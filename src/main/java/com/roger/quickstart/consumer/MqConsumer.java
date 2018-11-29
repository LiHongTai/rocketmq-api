package com.roger.quickstart.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class MqConsumer {

    public static void main(String[] args) throws Exception{
        //step1:设置消费者组名 -- 使用推送消息模式
        DefaultMQPushConsumer defaultMQPushConsumer =
                new DefaultMQPushConsumer("defaultMQPushConsumerGroup");
        //step2:设置消费者第一次启动是从队列的头部开始还是尾部开始
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //step3:设置nameServer服务
        //defaultMQPushConsumer.setNamesrvAddr("192.168.1.60:9786;192.168.1.64:9786");
        defaultMQPushConsumer.setNamesrvAddr("172.20.10.60:9876");
        //step4:指定订阅Topic和Tag
        defaultMQPushConsumer.subscribe("TopicQuickStart","TagA");
        //step5:注册消费者监听事件 -- 消息并发消费
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgExtList, ConsumeConcurrentlyContext context) {
                MessageExt msgExt = msgExtList.get(0);
                System.out.println(String.format("Consume message [%s],TagName [%s]",
                        new String(msgExt.getBody()),
                        msgExt.getTags()));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //step6:启动消费者
        defaultMQPushConsumer.start();
        System.out.println("Consumer Started.");
    }
}
