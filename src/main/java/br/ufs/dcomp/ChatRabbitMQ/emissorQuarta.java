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
import java.util.List;


public class emissorQuarta {
    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        
        // URI completa do CloudAMQP (Usuario:Senha@Host/VHost)
        factory.setUri("amqps://qynhcgcj:PpmRlIuA8oSOCN2wHCSP4R9dJf99WIJU@shark.rmq.cloudamqp.com/qynhcgcj");
        
        // CRÍTICO PARA ALTA DISPONIBILIDADE:
        // Permite reconexão automática caso um nó do cluster falhe
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000); // Tenta reconectar a cada 5 segundos

        // ---------------------------------------------

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        System.out.println("Conectado ao RabbitMQ via Load Balancer AWS");

        // Declara o exchange (deve ser o mesmo do receiver)
        // Nota: Exchanges não precisam de argumentos de Quorum, apenas as Filas (Queues).
        channel.exchangeDeclare("usuarios_direct", "direct", true);

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        Deque<String> pilha_de_requisicoes = new ArrayDeque<>();
        
        // Empilha os elementos
        pilha_de_requisicoes.push("");
        
        
        System.out.print("User: ");
        String meusuario = reader.readLine();
        String destinatario = "";
        String grupo = "";
        //String msg = "";

        byte[] body;

        while (true) {
        
            // Pega a ultima requisição da pilha
            System.out.print(pilha_de_requisicoes.peek() + ">> ");
            
            // Lê o comando do utilizador
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
            
            // Quando o primeiro char é uma exclamação !, deve-se criar um grupo ou comando
            else if (comando.charAt(0) == '!') {
                
                String[] tokens = comando.split(" ");
                
                if(tokens[0].equalsIgnoreCase("!addGroup")) {
                    grupo = tokens[1];

                    // Criação do Exchange para o grupo (Fanout)
                    channel.exchangeDeclare(grupo, "fanout", true);

                    // Se você não rodou o Receptor ainda, isso cria a fila agora para não dar erro.
                    // (Os parâmetros false, false, false, null DEVEM ser iguais aos do Receptor)
                    channel.queueDeclare(meusuario, false, false, false, null);

                    // Faz a amarração da fila do usuário atual (meusuario) com o grupo
                    channel.queueBind(meusuario, grupo, "");
                    System.out.println("Grupo criado e você (" + meusuario + ") foi adicionado a ele: " + grupo);
                }
                else if(tokens[0].equalsIgnoreCase("!addUser")){
                    
                    String usuarioParaAdicionar = tokens[1];
                    //String nomeDoGrupo = tokens[2];
                    
                    // Envia convite via Protobuf
                    byte[] buffer = serializacaoMensagem.getSerializa(meusuario, "", comando, null, "", "");
                    
                    channel.basicPublish("usuarios_direct", usuarioParaAdicionar, null, buffer);
                    System.out.println("Convite enviado para " + usuarioParaAdicionar);
                }
                else if(tokens[0].equalsIgnoreCase("!listGroups")){
                    System.out.println("Buscando grupos...");
                    // Chama a classe auxiliar passando o usuario atual
                    List<String> grupos = GerenciadorGrupo.listarGruposDoUsuario(meusuario);
                    
                    if (grupos.isEmpty()) {
                        System.out.println("Nenhum grupo encontrado.");
                    } else {
                        // Junta a lista numa string separada por virgula
                        System.out.println("Grupos: " + String.join(", ", grupos));
                    }
                }
                else if(tokens[0].equalsIgnoreCase("!listUsers")){
                    if(tokens.length < 2) {
                        System.out.println("Uso: !listUsers <nome_grupo>");
                    } else {
                        String nomeGrupo = tokens[1];
                        System.out.println("Buscando usuários de " + nomeGrupo + "...");
                        List<String> users = GerenciadorGrupo.listarUsuariosDoGrupo(nomeGrupo);
                        
                        if (users.isEmpty()) {
                            System.out.println("Nenhum usuário encontrado neste grupo.");
                        } else {
                            System.out.println("Usuários: " + String.join(", ", users));
                        }
                    }
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
                                        
                                        System.out.println("\nA enviar \"" + caminhoArquivo + "\" para " + destinoAtual + ".");
                                        
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
            
            // Quando o primeiro char é um cardinal #, deve-se conectar a um grupo
            else if (comando.charAt(0) == '#') {
                String restoEntrada = comando.substring(1).trim(); // remove o #
                String[] partes = restoEntrada.split(" ", 2);
                grupo = partes[0];

                // Atualiza o contexto (pilha) SEMPRE (para aparecer #grupo>> no prompt)
                pilha_de_requisicoes.push(("#"+grupo));
                
                // Só envia mensagem para o RabbitMQ se o usuário digitou algo além do nome do grupo
                if(partes.length > 1 && !partes[1].trim().isEmpty()) {
                    String mensagem = (partes.length > 1) ? partes[1] : "";
                    byte[] buffer  = serializacaoMensagem.getSerializa(meusuario, grupo, mensagem, null, "", "");  
                    channel.basicPublish(grupo, "", null, buffer);
                }
            }
                
            else if (comando.equalsIgnoreCase("/sair")) {
                break;
            }
            
            // Se o utilizador não digitou nenhum comando, começamos a logica de envio normal
            else {
                if(pilha_de_requisicoes.peek().charAt(0) == '@') {
                    if (!destinatario.isEmpty()) {
                        
                        byte[] buffer = serializacaoMensagem.getSerializa(meusuario, "", comando, null, "", "");
                        channel.basicPublish("usuarios_direct", destinatario, null, buffer);
                        
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
