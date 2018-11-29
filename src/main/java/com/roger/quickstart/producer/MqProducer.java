package com.roger.quickstart.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class MqProducer {

    private static int count = 100;

    public static void main(String[] args) throws Exception {

        //step1:设置生产者组名
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("producerGroupName");
        //step2:指定nameServer地址
        //defaultMQProducer.setNamesrvAddr("192.168.1.60:9876;192.168.1.64:9876");
        defaultMQProducer.setNamesrvAddr("172.20.10.60:9876");

        //step3:启动生产者
        defaultMQProducer.start();

        // 是否允许 Broker 自动创建Topic，建议线下开启，线上关闭
        //    如果  autoCreateTopicEnable=true 则不需要生产者主动创建Topic
        //    如果  autoCreateTopicEnable=false 则需要生产者主动创建Topic
        defaultMQProducer.createTopic("TopicQuickStart","TopicQuickStart",4);

        final Semaphore semaphore = new Semaphore(0);

        for (int i = 0; i < count; i++) {
            TimeUnit.MILLISECONDS.sleep(300);
            //step4:创建消息内容
            Message message = new Message("TopicQuickStart", "TagA",
                    ("Hello RocketMq " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            //step5:发送消息
            defaultMQProducer.send(message, new SendCallback() {

                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(
                            String.format("message [%s] send success.",
                                    new String(message.getBody())));
                    semaphore.release();
                }

                @Override
                public void onException(Throwable throwable) {
                    throwable.printStackTrace();
                }
            });
        }
        //申请
        semaphore.acquire(count + 1);
        //关闭生产者，释放资源
        defaultMQProducer.shutdown();

    }

}
