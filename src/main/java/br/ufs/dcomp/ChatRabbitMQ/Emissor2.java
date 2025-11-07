package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.BuiltinExchangeType;
import java.io.IOException;

public class Emissor2 {

    private static final String EXCHANGE_NAME = "usuarios_direct"; 
    private final Channel channel;

    
    public Emissor2(Connection connection) throws IOException {
        this.channel = connection.createChannel();
        
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT); 
    }

  
    public void sendMessage(String key, String menssage) throws IOException{
        channel.basicPublish(EXCHANGE_NAME, key, null, menssage.getBytes("UTF_8"));
    }
}


