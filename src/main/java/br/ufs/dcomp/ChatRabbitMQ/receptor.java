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

    System.out.println("🎧 Ouvindo mensagens para: " + nome_usuario);
    System.out.println("----------------------------------------");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, 
                               AMQP.BasicProperties properties, byte[] body) throws IOException {
        
        
        
          MensagemProto.Mensagem msg = MensagemProto.Mensagem.parseFrom(body);

          String emissor = msg.getEmissor();
          String data    = msg.getData();
          String hora    = msg.getHora();
          String grupo   = msg.getGrupo();

          MensagemProto.Conteudo c = msg.getConteudo();

          String tipo   = c.getTipo();
          String nome   = c.getNome();
          byte[] arquivo = c.getCorpo().toByteArray();

           // Exibe no console
          String mensagem = emissor + grupo + data + hora;
        //String remetente = envelope.getRoutingKey();
        
        //SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy 'às' HH:mm");
        //String dataHora = sdf.format(new Date());
        
        //System.out.println("(" + dataHora + ") @" + mensagem);
        // TODO implementar a logica de conexao com o grupo para que as mensagens dele sejam exibidas por usuarios 
        System.out.println(mensagem);
      }
    };

    channel.basicConsume(nome_usuario, true, consumer);
    
    // Mantém o programa rodando
    System.out.println("Pressione Ctrl+C para parar...");
    Thread.currentThread().join();
  }
}