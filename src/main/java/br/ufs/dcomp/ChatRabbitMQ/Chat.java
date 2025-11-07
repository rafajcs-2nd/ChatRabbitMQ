package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.*;

import java.io.IOException;


import java.io.BufferedReader;
import java.io.InputStreamReader;


public class Chat {
  
  /*
  private static void   criaFila(Channel channel) throws IOException {
    Scanner scanner = new Scanner(System.in);
    
    System.out.print("User: ");
    
    String nome_usuario = scanner.nextLine();
    
    channel.queueDeclare(nome_usuario, false,   false,     false,       null);

    scanner.close();
    
  }
  */

  public static void main(String[] argv) throws Exception {
    //System.out.println("Iniciou!");
    
    ConnectionFactory factory = new ConnectionFactory();
    
    factory.setHost("3.229.56.253");  // IP do RabbitMq Docker!
    factory.setUsername("coelhOS");   // Alterar nome
    factory.setPassword("senha");     // Alterar
    factory.setVirtualHost("/");
    
    //System.out.println("usuario e senha");
  
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    //System.out.println("conexao ok");
    
    // Declara o exchange principal para mensagens entre usuários
    channel.exchangeDeclare("usuarios_direct", "direct", true);

    System.out.print("User: ");
    
    // Le o nome do usuario e cria um fila para ele
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    String nome_usuario = reader.readLine();

    
    channel.queueDeclare(nome_usuario, false,   false,     false,       null);
    
    // Vincula a fila ao exchange criado
    channel.queueBind(nome_usuario, "usuarios_direct", nome_usuario);

    String destinatario = "";

    while (true) {

      System.out.print(destinatario + "<< ");
      String comando = reader.readLine();

      if(comando.isEmpty()) {
        continue;
      }

      if(comando.charAt(0) == '@') {
        destinatario = comando;
  
      }
      else {
        String mensagem = comando;
        // Publica a mensagem no exchage com a chave de rota destinatário
        channel.basicPublish("usuarios_direct", destinatario, null, mensagem.getBytes("UTF-8"));
        continue;
      }
      
      
    }
    
    /*
    String QUEUE_NAME = "minha-fila";
    
    //(queue-name, durable, exclusive, auto-delete, params); 
    channel.queueDeclare(QUEUE_NAME, false,   false,     false,       null);
    
    Consumer consumer = new DefaultConsumer(channel) {
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)           throws IOException {

        String message = new String(body, "UTF-8");
        System.out.println(message);

      }
    };
    
    //(queue-name, autoAck, consumer);    
    channel.basicConsume(QUEUE_NAME, true,    consumer);
    */
  }
}