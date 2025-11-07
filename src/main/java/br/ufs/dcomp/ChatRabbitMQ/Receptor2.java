package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.*;

import java.io.IOException;

public class Receptor2 {

 private static final String EXCHANGE_NAME = "usuarios_direct";
 private final Connection connection;
 
 public Receptor2(Connection connection){
     this.connection = connection;
 }
 
 public void comunicacaoON(String username) throws IOException{
    Channel channel = connection.createChannel();
    channel.queueDeclare(username, false, false, true, null);
    channel.queueBind(username, EXCHANGE_NAME, username);
    
    Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String fullMessage = new String(body, "UTF-8");
                
    }
    
    
 };
 channel.basicConsume(username, true, consumer);
 
}
}
