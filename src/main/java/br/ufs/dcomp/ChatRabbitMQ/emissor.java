package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import br.ufs.dcomp.ChatRabbitMQ.serializacaoMensagem;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;

public class emissor {
  
  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    //factory.setHost("54.146.215.199");
    //factory.setUsername("coelhOS");
    //factory.setPassword("senha");
    //factory.setVirtualHost("/");
    factory.setUri("amqps://jtvssape:i4MuhTgXn_97dmaZe9Uz3-WnPsu53HgL@shark.rmq.cloudamqp.com/jtvssape");
  
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    System.out.println("conecta ao rebbitMQ");

    // Declara o exchange (deve ser o mesmo do receiver)
    channel.exchangeDeclare("usuarios_direct", "direct", true);

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    
    Deque<String> pilha_de_requisicoes = new ArrayDeque<>();
    
    // Empilha os elementos
    pilha_de_requisicoes.push("");
    
    
    System.out.print("User: ");
    String meusuario = reader.readLine();
    String destinatario = "";
    String grupo = "";
    String msg = "";
    
    FileOutputStream fos = new FileOutputStream(new File("msg.bin"));
    
    byte[] body;

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
            // Define o destinatario
            destinatario = comando.substring(1); // Remove o '@'
            pilha_de_requisicoes.push(("@"+destinatario));
        }
        
        // Quando o primeiro char é uma interrogação !, deve-se criar um grupo
        else if (comando.charAt(0) == '!') {
            System.out.println("vai criar um grupo");
            String[] tokens = comando.split(" ");
            
            if(tokens[0].equalsIgnoreCase("!addGroup")) {
                String addGroup = tokens[0];
                grupo = tokens[1];
                
                System.out.println("addGroup: " + addGroup +" grupo: "+ grupo);
                
                channel.exchangeDeclare(grupo, "fanout", true);
                
                System.out.println("Grupo criado: " + grupo);
            }
            else {
                channel.basicPublish("usuarios_direct", tokens[1], null, comando.getBytes("UTF-8"));
            }
        }
        
         // Quando o primeiro char é um sustenido #, deve-se conectar a um grupo
        else if (comando.charAt(0) == '#') {
            String restoEntrada = comando.substring(1).trim(); // remove o #
            
        
            String[] partes = restoEntrada.split(" ", 2);
            grupo = partes[0];
            String mensagem = (partes.length > 1) ? partes[1] : "";
            
            fos = serializacaoMensagem.getSerializaGrupo(meusuario, grupo, mensagem);
           // msg = meusuario + "#" + grupo +  " diz: " + mensagem;
            pilha_de_requisicoes.push(("#"+grupo));
    
        }
            
        else if (comando.equalsIgnoreCase("/sair")) {
            break;
        }
        
        // Se o usuario não digitou nenhum comando, começamos a logica 
        else {
            if(pilha_de_requisicoes.peek().charAt(0) == '@') {
                if (!destinatario.isEmpty()) {
                    String mensagem = comando;
                    String mensagemCompleta = meusuario + " diz: " + mensagem;
            
                    // Publica a mensagem no exchange com routing key = destinatário
                    channel.basicPublish("usuarios_direct", destinatario, null, mensagemCompleta.getBytes("UTF-8"));
                    pilha_de_requisicoes.push(comando);
                    continue;
                } else {
                    System.out.println("Primeiro defina um destinatário com @usuario");
                    continue;
                }                
            }
            else if(pilha_de_requisicoes.peek().charAt(0) == '#') {
                body = Files.readAllBytes(Paths.get("msg.bin"));
                channel.basicPublish(grupo, "", null, body);
                
            }
        }
       
    }

    channel.close();
    connection.close();
    reader.close();
  }

}