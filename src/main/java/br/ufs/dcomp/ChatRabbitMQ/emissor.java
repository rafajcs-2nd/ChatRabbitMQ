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
    
    while (true) {
    
        // Pega a ultima requisição da pilha
        System.out.print(pilha_de_requisicoes.peek() + ">> ");
        
        // Lê o comando do usuário
        String comando = reader.readLine();
        
        // Caso o comando seja vazio
        if (comando == null || comando.isEmpty()) {
            continue;
        }
        
        // Quando o primeiro char é um @, deve enviar mensagens diretamente ao destinatário  
        if (comando.charAt(0) == '@') {
            // Define o destinatario
            destinatario = comando.substring(1); // Remove o '@'
            pilha_de_requisicoes.push(("@"+destinatario));
        }
        
        // Quando o primeiro char é uma exclamação !, deve-se criar um grupo ou adicionar um usuário
        else if (comando.charAt(0) == '!') {
            
            String[] tokens = comando.split(" ");
            
            // cria um grupo
            if(tokens[0].equalsIgnoreCase("!addGroup")) {
                
                grupo = tokens[1];
                channel.exchangeDeclare(grupo, "fanout", true);
                System.out.println("Grupo criado: " + grupo);
            }
            // adiciona um usuário a um grupo
            else if(tokens[0].equalsIgnoreCase("!addUser")){
                
                String usuarioParaAdicionar = tokens[1];
                grupo = tokens[2];
                
                byte[] buffer = serializacaoMensagem.getSerializaGrupo(meusuario, "", comando);
                
                channel.basicPublish("usuarios_direct", usuarioParaAdicionar, null, buffer);
                System.out.println("Usuário " + usuarioParaAdicionar + " adicionado a " + grupo);
            }
            // remove um grupo
            else if(tokens[0].equalsIgnoreCase("!removeGroup")) {
                
                grupo = tokens[1];
                System.out.println("Grupo" + grupo + "está sendo excluído!");
                channel.exchangeDelete(grupo);
                System.out.println("Grupo excluído: " + grupo);
                pilha_de_requisicoes.push("");
            }
            // adiciona um usuário a um grupo
            else if(tokens[0].equalsIgnoreCase("!removeUser")){
                
                String usuarioParaRemover = tokens[1];
                grupo = tokens[2];
                
                channel.queueUnbind(usuarioParaRemover, grupo, "");
                System.out.println("Usuário " + usuarioParaRemover + " removido de " + grupo);
            }
        }
        
         // Quando o primeiro char é um sustenido #, deve-se conectar a um grupo
        else if (comando.charAt(0) == '#') {
            String restoEntrada = comando.substring(1).trim(); // remove o #
            
        
            String[] partes = restoEntrada.split(" ", 2);
            grupo = partes[0];
            String mensagem = (partes.length > 1) ? partes[1] : "";
            
            byte[] buffer  = serializacaoMensagem.getSerializaGrupo(meusuario, grupo, mensagem);
            
            try {
                channel.basicPublish(grupo, "", null, buffer);
                pilha_de_requisicoes.push(("#"+grupo));
            }
            catch(Exception e) {
                System.out.println("Erro ao se conectar ao grupo");
            }
        }
            
        else if (comando.equalsIgnoreCase("/sair")) {
            break;
        }
        
        // Se o usuario não digitou nenhum comando, começamos a logica 
        else {
            if(pilha_de_requisicoes.peek().isEmpty()) {
                continue;
            }
            if(pilha_de_requisicoes.peek().charAt(0) == '@') {
                if (!destinatario.isEmpty()) {
                    
                    byte[] buffer = serializacaoMensagem.getSerializaGrupo(meusuario, "", comando);
                    channel.basicPublish("usuarios_direct", destinatario, null, buffer);

                } else {
                    System.out.println("Primeiro defina um destinatário com @usuario");
                    
                }                
            }
            else if(pilha_de_requisicoes.peek().charAt(0) == '#') {
                byte[] buffer = serializacaoMensagem.getSerializaGrupo(meusuario, grupo, comando);
                try {
                    channel.basicPublish(grupo, "", null, buffer);
                }
                catch(Exception e) {
                    System.out.println("Erro ao enviar mensagem. O grupo " +grupo+ " não existe!" );
                }
            }
            else {
                continue;
            }
        }
       
    }

    channel.close();
    connection.close();
    reader.close();
  }

}