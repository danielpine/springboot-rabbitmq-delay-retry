package io.danielpine.delayqueue.producer;

import io.danielpine.delayqueue.conf.RabbitConfiguration;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.UUID;

@RestController("/api")
public class Producer {

    @Resource
    private RabbitTemplate rabbitTemplate;
    @Resource
    MessagePostProcessor correlationIdProcessor;

    @RequestMapping("/send")
    String sendDirect(@RequestParam String message) throws Exception {
        System.out.println("开始生产");
        CorrelationData data = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend(RabbitConfiguration.WORKING_EXCHANGE, RabbitConfiguration.WORKING_DEMO_ROUTINGKEY,
                message, correlationIdProcessor, data);
        System.out.println("结束生产");
        System.out.println("发送id:" + data);
        return "OK,sendDirect:" + message;
    }
}

