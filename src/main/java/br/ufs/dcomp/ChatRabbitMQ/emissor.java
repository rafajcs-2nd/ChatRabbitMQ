package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.ArrayDeque;
import java.util.Deque;

public class emissor {
  
  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("3.229.56.253");
    factory.setUsername("coelhOS");
    factory.setPassword("senha");
    factory.setVirtualHost("/");
  
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    // Declara o exchange (deve ser o mesmo do receiver)
    channel.exchangeDeclare("usuarios_direct", "direct", true);

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    
    Deque<String> pilha_de_requisicoes = new ArrayDeque<>();
    
    // Empilha os elementos
    pilha_de_requisicoes.push("");
    
    
    System.out.print("User: ");
    String meusuario = reader.readLine();
    String destinatario = "";
    String comando_formatado = "";

    while (true) {
    
        // Pega a ultima requisição da pilha
        System.out.print(pilha_de_requisicoes.peek() + ">> ");
        
        // Lê o comando do usuário
        String comando = reader.readLine();
        
        // Caso o comando seja vazio
        if (comando == null || comando.isEmpty()) {
            continue;
        }
        
        // Quando o primeiro char é um @, deve  
        if (comando.charAt(0) == '@') {
            destinatario = comando.substring(1); // Remove o '@'
                
            if (!destinatario.isEmpty()) {
                String mensagem = comando;
                String mensagemCompleta = meusuario + " diz: " + mensagem;
        
                // Publica a mensagem no exchange com routing key = destinatário
                channel.basicPublish("usuarios_direct", destinatario, null, mensagemCompleta.getBytes("UTF-8"));
            } else {
                System.out.println("Primeiro defina um destinatário com @usuario");
            }
        }
        
        // Quando o primeiro char é uma interrogação !, deve-se criar um grupo
        else if (comando.charAt(0) == '!') {
            String[] tokens = comando.split(" ");
            
            String addGroup = tokens[0];
            String usuario = tokens[1];
            String grupo = tokens[2];
            
            channel.exchangeDeclare(grupo, "fanout", true);
            System.out.println("Grupo criado: " + grupo);
            // Cria um grup
        }
        
         // Quando o primeiro char é um sustenido #, deve-se conectar a um grupo
        else if (comando.charAt(0) == '#') {
            String restoEntrada = comando.substring(1).trim(); // remove o #
            
        
            String[] partes = restoEntrada.split(" ", 2);
            String grupo = partes[0];
            String mensagem = (partes.length > 1) ? partes[1] : "";

            String msg = meusuario + "#" + grupo +  " diz: " + mensagem;
            channel.basicPublish(grupo, "", null, msg.getBytes("UTF-8"));
            // Se conecta ao grupo
        }
            
        else if (comando.equalsIgnoreCase("/sair")) {
            break;
        }
       
    }

    channel.close();
    connection.close();
    reader.close();
  }
}