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
    //factory.setHost("3.229.56.253");
    //factory.setUsername("coelhOS");
    //factory.setPassword("senha");
    //factory.setVirtualHost("/");
    factory.setUri("amqps://jtvssape:i4MuhTgXn_97dmaZe9Uz3-WnPsu53HgL@shark.rmq.cloudamqp.com/jtvssape");
  
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    System.out.print("User: ");
    String nome_usuario = reader.readLine();
    
    channel.queueDeclare(nome_usuario, false, false, false, null);
    channel.queueBind(nome_usuario, "usuarios_direct", nome_usuario);

    System.out.println("Ouvindo mensagens para: " + nome_usuario);
    System.out.println("----------------------------------------");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        
        
        
          MensagemProto.Mensagem msg = MensagemProto.Mensagem.parseFrom(body);
          MensagemProto.Conteudo c = msg.getConteudo();

          String emissor = msg.getEmissor();
          String data    = msg.getData();
          String hora    = msg.getHora();
          String grupo   = msg.getGrupo();

          String tipo   = c.getTipo();
          String nome   = c.getNome();
          byte[] arquivo = c.getCorpo().toByteArray();

          
          String textoMsg = new String(msg.getConteudo().getCorpo().toByteArray(), "UTF-8");
          
          if(textoMsg.startsWith("!addUser")){
            String[] partes = textoMsg.split(" ");
            String nomeDoGrupo = partes[2];
            
            channel.queueBind(nome_usuario, nomeDoGrupo, "");
            System.out.println("Você foi adicionado a " + nomeDoGrupo);
          }
          else if(!grupo.isEmpty()){
            System.out.println("(" + msg.getData() + " às " + msg.getHora() + ") " + msg.getEmissor() + "#" + grupo + " diz: " + textoMsg);
          }
          else{
            String prefixo = grupo.isEmpty() ? "" : " para #" + grupo;
            System.out.println("(" + msg.getData() + " às " + msg.getHora() + ") " + msg.getEmissor() + " diz: " + textoMsg);
            
          }
        
      }
    };

    channel.basicConsume(nome_usuario, true, consumer);
    
    // Mantém o programa rodando
    System.out.println("Pressione Ctrl+C para parar...");
    Thread.currentThread().join();
  }
}