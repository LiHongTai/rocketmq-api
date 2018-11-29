package com.roger.order.producer;

import com.roger.order.entity.OrderMsgDTO;
import com.roger.utils.SnowflakeIdWorker;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.List;

public class OrderMqProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer defaultMQProducer =
                new DefaultMQProducer("orderMQProducerGroup");
        defaultMQProducer.setNamesrvAddr("172.20.10.60:9876");
        defaultMQProducer.start();
        defaultMQProducer.createTopic("OrderTopic", "OrderTopic", 3);

        String[] tags = new String[]{"TagC", "TagP", "TagF"};

        List<OrderMsgDTO> orderMsgList = new ArrayList<>();
        int orderCount = 5;
        for (int i = 0; i < orderCount; i++) {
            long orderId = SnowflakeIdWorker.getInstance().nextId();
            OrderMqProducer.builderOrderMsgList(orderMsgList, orderId);
        }

        for (int i = 0; i < orderMsgList.size(); i++) {
            OrderMsgDTO orderMsgDTO = orderMsgList.get(i);
            String body = orderMsgDTO.toString();
            Message msg = new Message("OrderTopic",
                    tags[i % tags.length],
                    "OrderKey" + i,
                    body.getBytes(RemotingHelper.DEFAULT_CHARSET));

            SendResult sendResult = defaultMQProducer.send(msg, new MessageQueueSelector() {

                //List<MessageQueue> msgQueList 消息要发送的Topic下的所有分区
                //Message message 消息对象
                // Object args 额外的参数，用户可以自己传递参数
                // 比如为了把同一个订单的消息发送到同一个分区中，
                // 可以把订单号作为一个参数传递过去然后mod分区个数，
                // 就可以保证把同一个订单的消息发送到同一个分区中去
                @Override
                public MessageQueue select(List<MessageQueue> msgQueList, Message message, Object args) {
                    long orderId = (long) args;
                    long index = orderId % msgQueList.size();
                    return msgQueList.get((int) index);
                }
            }, orderMsgDTO.getOrderId());

            System.out.println(sendResult +
                    String.format("message [%s] send success.",
                            new String(msg.getBody())));
        }
        //defaultMQProducer.shutdown();
    }


    private static void builderOrderMsgList(List<OrderMsgDTO> orderMsgList, long orderId) {
        orderMsgList.add(new OrderMsgDTO(orderId, "Create"));
        orderMsgList.add(new OrderMsgDTO(orderId, "PayOff"));
        orderMsgList.add(new OrderMsgDTO(orderId, "Finish"));
    }
}
