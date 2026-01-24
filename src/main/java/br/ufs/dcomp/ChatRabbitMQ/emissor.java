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
            
            String[] tokens = comando.split(" ");
            
            if(tokens[0].equalsIgnoreCase("!addGroup")) {
                
                grupo = tokens[1];
                channel.exchangeDeclare(grupo, "fanout", true);
                System.out.println("Grupo criado: " + grupo);
            }
            else if(tokens[0].equalsIgnoreCase("!addUser")){
                
                String usuarioParaAdicionar = tokens[1];
                String nomeDoGrupo = tokens[2];
                
                byte[] buffer = serializacaoMensagem.getSerializa(meusuario, "", comando, null, "", "");
                
                channel.basicPublish("usuarios_direct", usuarioParaAdicionar, null, buffer);
                System.out.println("Convite enviado para " + usuarioParaAdicionar);
            }
            else if(tokens[0].equalsIgnoreCase("!upload")){
                String caminhoArquivo = tokens[1];
                String destinoAtual = pilha_de_requisicoes.peek();
                
                if(destinoAtual.equals("")){
                    System.out.println("Erro: Defina um destino com @usuario ou #grupo antes de enviar arquivos.");
                }
                else {
                    Thread threadUpload = new Thread(() -> {
                        try {
                            File arquivo = new File(caminhoArquivo);
                                if (arquivo.exists() && !arquivo.isDirectory()) {
                                    byte[] bytesArquivo = Files.readAllBytes(Paths.get(caminhoArquivo));
                                    
                                    System.out.println("\nEnviando \"" + caminhoArquivo + "\" para " + destinoAtual + ".");
                                    
                                    String tipoMime = Files.probeContentType(Paths.get(caminhoArquivo));
                                    
                                    String nomeGrupo = comando.startsWith("#") ? comando.substring(1) : "";
                                    byte[] buffer = serializacaoMensagem.getSerializa(meusuario, nomeGrupo, "", bytesArquivo, arquivo.getName(), tipoMime);
                                    
                                    
                                    if(comando.startsWith("@")){
                                        channel.basicPublish("usuarios_direct", comando.substring(1), null, buffer);
                                    }
                                    else if(comando.startsWith("#")){
                                        channel.basicPublish(comando.substring(1), "", null, buffer);
                                    }
                    
                                    System.out.println("Arquivo \"" + caminhoArquivo + "\" foi enviado para " + destinoAtual + " !");
                                    System.out.print(pilha_de_requisicoes.peek() + ">> ");
                                }
                                else {
                                    System.out.println("\n[Erro] Arquivo não encontrado: " + caminhoArquivo);
                                }
                            } catch (Exception e) {
                                System.err.println("\n[Erro no Upload] " + e.getMessage());
                            }});
                            
                            threadUpload.start();
                    }
            }
        }
        
         // Quando o primeiro char é um sustenido #, deve-se conectar a um grupo
        else if (comando.charAt(0) == '#') {
            String restoEntrada = comando.substring(1).trim(); // remove o #
            
        
            String[] partes = restoEntrada.split(" ", 2);
            grupo = partes[0];
            String mensagem = (partes.length > 1) ? partes[1] : "";
            
            byte[] test;
            
            byte[] buffer  = serializacaoMensagem.getSerializa(meusuario, grupo, mensagem, null, "", "");
            
            channel.basicPublish(grupo, "", null, buffer);
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
                    
                    byte[] buffer = serializacaoMensagem.getSerializa(meusuario, "", comando, null, "", "");
                    channel.basicPublish("usuarios_direct", destinatario, null, buffer);
                    //pilha_de_requisicoes.push(comando); n funciona
                    
                } else {
                    System.out.println("Primeiro defina um destinatário com @usuario");
                    
                }                
            }
            else if(pilha_de_requisicoes.peek().charAt(0) == '#') {
                byte[] buffer = serializacaoMensagem.getSerializa(meusuario, grupo, comando, null, "", "");
                channel.basicPublish(grupo, "", null, buffer);
                
            }
        }
       
    }

    channel.close();
    connection.close();
    reader.close();
  }

}