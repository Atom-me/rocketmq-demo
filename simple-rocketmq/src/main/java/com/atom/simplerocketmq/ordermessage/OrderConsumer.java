package com.atom.simplerocketmq.ordermessage;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * 消费顺序消息
 *
 * @author Atom
 */
public class OrderConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("simple_consumer_group");

        consumer.setNamesrvAddr("10.16.118.230:9876");

        // 订阅，可以订阅多个主题，多写几个
        // 订阅多个标签TagA、TagB、TagC：consumer.subscribe("topic_demo","TagA || TagB || TagC");
        // 订阅所有标签：consumer.subscribe("topic_demo","*");
        consumer.subscribe("topic_order_demo", "tag_demo");

        // 设置消费者一次拉取消息最大数
        consumer.setConsumeMessageBatchMaxSize(2);


        //创建消息监听,使用顺序消费监听
        consumer.setMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                //读取消息
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
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });


        // 设置完 消息监听，然后在启动 消费者
        consumer.start();


    }
}
