package com.atom.simplerocketmq.broadcastmessage;

import lombok.SneakyThrows;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * @author Atom
 */
public class BroadcastConsumerA {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("broadcast_producer_group");

        consumer.setNamesrvAddr("10.16.118.230:9876");

        // 订阅，可以订阅多个主题，多写几个
        // 订阅多个标签TagA、TagB、TagC：consumer.subscribe("topic_broadcast_demo","TagA || TagB || TagC");
        // 订阅所有标签：consumer.subscribe("topic_broadcast_demo","*");
        consumer.subscribe("topic_broadcast_demo", "tag_demo");

        // 设置消费者一次拉取消息最大数
        consumer.setConsumeMessageBatchMaxSize(2);

        // 默认是 CLUSTERING 模式，这里设置为 广播模式：BROADCASTING
        // 同一个消费者组里面的消费者都可以消费消息
        consumer.setMessageModel(MessageModel.BROADCASTING);


        //创建消息监听
        consumer.setMessageListener(new MessageListenerConcurrently() {
            @SneakyThrows
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        String topic = msg.getTopic();
                        String tags = msg.getTags();
                        String msgId = msg.getMsgId();
                        byte[] body = msg.getBody();
                        String msgBody = new String(body, RemotingHelper.DEFAULT_CHARSET);

                        System.err.printf("receive message : topic=%s ,tags=%s , msgId=%s , body=%s%n", topic, tags, msgId, msgBody);
                    } catch (Exception e) {

                        // 消息消费失败，重试
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                // 消息成功消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });


        // 设置完 消息监听，然后在启动 消费者
        consumer.start();


    }
}
