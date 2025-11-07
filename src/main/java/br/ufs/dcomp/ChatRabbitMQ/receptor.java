package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

public class receptor {
  
  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("3.229.56.253");
    factory.setUsername("coelhOS");
    factory.setPassword("senha");
    factory.setVirtualHost("/");
  
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    System.out.print("User: ");
    String nome_usuario = reader.readLine();
    
    channel.queueDeclare(nome_usuario, false, false, false, null);
    channel.queueBind(nome_usuario, "usuarios_direct", nome_usuario);

    System.out.println("🎧 Ouvindo mensagens para: " + nome_usuario);
    System.out.println("----------------------------------------");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, 
                               AMQP.BasicProperties properties, byte[] body) throws IOException {
        
        String mensagem = new String(body, "UTF-8");
        String remetente = envelope.getRoutingKey();
        
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy 'às' HH:mm");
        String dataHora = sdf.format(new Date());
        
        System.out.println("(" + dataHora + ") @" + remetente + " diz: " + mensagem);
      }
    };

    channel.basicConsume(nome_usuario, true, consumer);
    
    // Mantém o programa rodando
    System.out.println("Pressione Ctrl+C para parar...");
    Thread.currentThread().join();
  }
}