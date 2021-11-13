package com.atom.rocketproducer;

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.context.junit4.SpringRunner;


/**
 * 注意：
 * <p>
 * 1、跟原生rocketmq Api对比，rocketMQ start 的做了二次的封装，把同步异步发送消息，用方法名称做了区别；
 * 相同的是，无论是原生的api还是二次封装的api，异步调用的时候，回调是在参数体里的，毕竟异步发送需要等待回调，而同步发送可以只有回调。
 * <p>
 * 2、顺序消息：严格顺序消息模式下，消费者收到的所有消息均是有顺序的
 * 发送消息的时候，消息被存储在MessageQueue队列里的，默认的时候，是4个队列；为了保证消息的顺序，是需要把相同业务的数据按照顺序写入对应的队列中，单个队列下，数据是严格有序的；
 * <p>
 * rocketMQ starter 对原生api做了二次封装，提供了默认的MessageQueue选择器，用的字符串的hash算法实现的，如果不满足实际需求，需要重写选择器。
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {RocketProducerApplication.class})
class RocketProducerApplicationTests {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    /**
     * 同步消息
     */
    @Test
    public void syncSend() {
        for (int i = 0; i < 100; i++) {
            Message<String> bashMessage = new GenericMessage<String>("test_producer syncSend message " + i);
            SendResult syncSend = rocketMQTemplate.syncSend("test_producer", bashMessage);
            System.out.println(syncSend);
        }
    }

    /**
     * 异步消息
     */
    @Test
    public void asyncSend() {
        for (int i = 0; i < 100; i++) {
            Message<String> message = new GenericMessage<String>("test_producer asyncSend message " + i);
            rocketMQTemplate.asyncSend("test_producer", message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    System.out.println("发送失败");
                }
            });
        }
    }


    /**
     * 单向发送消息
     */
    @Test
    public void sendOneWay() {
        for (int i = 0; i < 100; i++) {
            Message<String> message = new GenericMessage<String>("test_producer sendOneWay message " + i);
            rocketMQTemplate.sendOneWay("test_producer", message);
            System.out.println("只发送一次");
        }
    }

    /**
     * 发送有序消息
     */
    @Test
    public void syncSendOrder() {
        String[] tags = new String[]{"TagA", "TagC", "TagD"};

        for (int i = 0; i < 10; i++) {
            // 加个时间前缀
            Message<String> message = new GenericMessage<String>("我是顺序消息" + i);
            SendResult sendResult = rocketMQTemplate.syncSendOrderly("test_producer:" + tags[i % tags.length], message,
                    i + "");
            System.out.println(sendResult);
        }
    }
}
