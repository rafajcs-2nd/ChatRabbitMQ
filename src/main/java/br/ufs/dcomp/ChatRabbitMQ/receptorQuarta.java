package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

public class receptorQuarta {
  
public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    
    // URI completa do CloudAMQP (Usuario:Senha@Host/VHost)
    factory.setUri("amqps://qynhcgcj:PpmRlIuA8oSOCN2wHCSP4R9dJf99WIJU@shark.rmq.cloudamqp.com/qynhcgcj");
 
    
    // CRÍTICO: Se o Load Balancer te jogar para outro nó, ele reconecta sozinho
    factory.setAutomaticRecoveryEnabled(true);
    factory.setNetworkRecoveryInterval(5000); 

    // --------------------------------------
  
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    System.out.print("User: ");
    String nome_usuario = reader.readLine();
    
    // Garante que o Exchange existe (Evita erro se o receptor entrar antes do emissor)
    channel.exchangeDeclare("usuarios_direct", "direct", true);

    // Declaração da Fila
    // Mantemos 'null' nos argumentos para criar uma fila Clássica (padrão e robusta)
    channel.queueDeclare(nome_usuario, false, false, false, null);
    
    // Faz o Bind (Amarração) da fila com o Exchange
    channel.queueBind(nome_usuario, "usuarios_direct", nome_usuario);

    System.out.println("🎧 Ouvindo mensagens para: " + nome_usuario);
    System.out.println("----------------------------------------");
    

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
          try {
             // Parse da mensagem usando Protocol Buffers
             MensagemProto.Mensagem msg = MensagemProto.Mensagem.parseFrom(body);
             MensagemProto.Conteudo c = msg.getConteudo();
      
             String emissor     = msg.getEmissor();
             String data        = msg.getData();
             String hora        = msg.getHora();
             String grupo       = msg.getGrupo();
             String nomeArquivo = c.getNome();
             byte[] dadosArquivo = c.getCorpo().toByteArray();
      
             // Lógica de Recebimento de Arquivo
             if (nomeArquivo != null && !nomeArquivo.isEmpty()) {
                 String diretorioDownloads = "downloads/";
                 
                 File pastaDownload = new File(diretorioDownloads);
                 if (!pastaDownload.exists()) pastaDownload.mkdirs();
                 
                 File arquivoDestino = new File(diretorioDownloads + nomeArquivo);
                 try (FileOutputStream fos = new FileOutputStream(arquivoDestino)) {
                     fos.write(dadosArquivo);
                 }
                 
                 System.out.println("\n(" + data + " às " + hora + ") Arquivo \"" + nomeArquivo + "\" recebido de @" + emissor + "!");
                 System.out.print(nome_usuario + ">> ");
             }
             else {
                 // Lógica de texto
                 String textoMsg = new String(msg.getConteudo().getCorpo().toByteArray(), "UTF-8");

                 // Ignora mensagens vazias
                 if (textoMsg.trim().isEmpty()) {
                     return;
                 }
                 
                 // Lógica de adicionar ao grupo (!addUser)
                 if(textoMsg.startsWith("!addUser")){
                    String[] partes = textoMsg.split(" ");
                    // Formato esperado: !addUser usuario nomeGrupo
                    if (partes.length >= 2) {
                        // O usuarioNome é este receptor, então pegamos o grupo (índice 2)
                        String nomeDoGrupo = partes.length > 2 ? partes[2] : partes[1];
                        
                        // O receptor faz o bind da sua própria fila ao Exchange do grupo
                        channel.exchangeDeclare(nomeDoGrupo, "fanout", true); // Garante que existe
                        channel.queueBind(nome_usuario, nomeDoGrupo, "");
                        
                        System.out.println("\n[Info] Você foi adicionado ao grupo #" + nomeDoGrupo);
                        System.out.print(nome_usuario + ">> ");
                    }
                }
                  
                else if(!grupo.isEmpty()){
                  System.out.println("\n(" + msg.getData() + " às " + msg.getHora() + ") " + msg.getEmissor() + "#" + grupo + " diz: " + textoMsg);
                  System.out.print(nome_usuario + ">> ");
                }
                else{
                  System.out.println("\n(" + msg.getData() + " às " + msg.getHora() + ") " + msg.getEmissor() + " diz: " + textoMsg);
                  System.out.print(nome_usuario + ">> ");
                }
             }
          } catch (Exception e) {
              System.out.println("\n[Erro ao processar mensagem] " + e.getMessage());
          }
      }
    };

    channel.basicConsume(nome_usuario, true, consumer);
    
    // Mantém o programa rodando
    // System.out.println("Pressione Ctrl+C para parar...");
    // Thread.currentThread().join(); // Não é estritamente necessário se o main não terminar, mas mal não faz.
  }
}
