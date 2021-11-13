package com.atom.simplerocketmq;


import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * 生产者生产消息，topic是TopicTest，tags是TagA，key没设置，message是Hello RocketMQ i
 *
 * @author Atom
 **/

@Component
@Order(1)
public class RocketMQProducer implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("groupProducer");
            producer.setNamesrvAddr("10.16.118.230:9876");//MQ服务器地址
            producer.setVipChannelEnabled(false);
            producer.start();
            for (int i = 0; i < 2; i++) {
                Message msg = new Message("TopicTest", "TagA", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.send(msg);
                System.out.printf("%s%n", sendResult);
                System.out.println("--");
            }
            // 关闭生产者
            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

