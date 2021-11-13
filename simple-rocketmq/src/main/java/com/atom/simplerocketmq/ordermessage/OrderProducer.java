package com.atom.simplerocketmq.ordermessage;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * 发送顺序消息
 *
 * @author Atom
 */
public class OrderProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("simple_producer_group");

        producer.setNamesrvAddr("10.16.118.230:9876");

        producer.start();

        // 连续发多条消息
        for (int i = 0; i < 5; i++) {
            // 创建消息
            Message message = new Message(
                    "topic_order_demo",// topic,会自动创建，rocketmq 配置文件中配置了自动创建topic ： autoCreateTopicEnable=true
                    "tag_demo",// tag ,主要用于消息过滤
                    "keys_1" + i,//消息的唯一值
                    ("hello, atom" + i).getBytes(RemotingHelper.DEFAULT_CHARSET)// 消息内容
            );
            // 发送消息，发送顺序消息，使用MessageQueueSelector 指定队列，包含三个参数：
            // message ：消息对象
            // MessageQueueSelector: 选择指定的消息队列对象（会将所有消息队列传入进来）
            // arg: 指定对应的队列下标，这个参数会传给 MessageQueueSelector#select()
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            //获取对应队列的下标
                            Integer index = (Integer) arg;
                            // 获取对应下标的队列
                            return mqs.get(index);
                        }
                    },
                    0);

            System.err.println("send result ::: " + sendResult);
        }


        // 关闭 producer
        producer.shutdown();

    }
}
