package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class emissor {
  
  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    //factory.setHost("shark.rmq.cloudamqp.com");
    //factory.setUsername("jtvssape");
    //factory.setPassword("i4MuhTgXn_97dmaZe9Uz3-WnPsu53HgL");
    factory.setUri("amqps://jtvssape:i4MuhTgXn_97dmaZe9Uz3-WnPsu53HgL@shark.rmq.cloudamqp.com/jtvssape");
    //factory.setVirtualHost("/");
  
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    // Declara o exchange (deve ser o mesmo do receiver)
    channel.exchangeDeclare("usuarios_direct", "direct", true);

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    
    System.out.print("User: ");
    
    String meusuario = reader.readLine();

    String destinatario = "";

    while (true) {
        if (!destinatario.isEmpty()) {
            System.out.print("@" + destinatario + ">> ");
        } else {
            System.out.print(">> ");
        }
        
        String comando = reader.readLine();

        if (comando == null || comando.isEmpty()) {
            continue;
        }

        if (comando.charAt(0) == '@') {
            destinatario = comando.substring(1); // Remove o '@'
        } else if (comando.equalsIgnoreCase("/sair")) {
            break;
        } else {
            if (!destinatario.isEmpty()) {
                String mensagem = comando;
                String mensagemCompleta = meusuario + " diz: " + mensagem;
        
                // Publica a mensagem no exchange com routing key = destinatário
                channel.basicPublish("usuarios_direct", destinatario, null, mensagemCompleta.getBytes("UTF-8"));
            } else {
                System.out.println("Primeiro defina um destinatário com @usuario");
            }
        }
    }

    // Limpeza
    channel.close();
    connection.close();
    reader.close();
  }
}