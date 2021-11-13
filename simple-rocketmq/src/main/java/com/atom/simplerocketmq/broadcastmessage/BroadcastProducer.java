package com.atom.simplerocketmq.broadcastmessage;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * 广播生产者，也可以批量发送
 *
 * @author Atom
 */
public class BroadcastProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("broadcast_producer_group");

        producer.setNamesrvAddr("10.16.118.230:9876");

        producer.start();

        // 创建10条消息
        List<Message> messageList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message message = new Message(
                    "topic_broadcast_demo",// topic,会自动创建，rocketmq 配置文件中配置了自动创建topic ： autoCreateTopicEnable=true
                    "tag_demo",// tag ,主要用于消息过滤
                    "keys_1" + i,//消息的唯一值
                    ("hello, atom" + i).getBytes(RemotingHelper.DEFAULT_CHARSET)// 消息内容
            );
            messageList.add(message);
        }


        // 批量发送消息
        SendResult sendResult = producer.send(messageList);

        System.err.println("send result ::: " + sendResult);

        // 关闭 producer
        producer.shutdown();

    }
}
