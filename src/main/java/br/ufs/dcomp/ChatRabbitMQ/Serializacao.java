package br.ufs.dcomp.ChatRabbitMQ;

import java.io.FileOutputStream;
import java.io.File;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;

public class Serializacao {
    
    public static void main(String[] args) throws Exception {
        
        byte[] corpoTeste = new byte[32];

        
        // Dados do conteúdo da mensagem
        MensagemProto.Conteudo imagemTeste = MensagemProto.Conteudo.newBuilder()
                .setTipo("image/png")
                .setCorpo(ByteString.copyFrom(corpoTeste))
                .setNome("minhaFoto.png")
                .build();


        // Dados da mensagem
        MensagemProto.Mensagem mensagemTeste = MensagemProto.Mensagem.newBuilder()
                .setEmissor("Rafael")
                .setData("13-11-2025")
                .setHora("14:12")
                .setGrupo("Grupo I")
                .setConteudo(imagemTeste)
                .build();

        // Serializando a mensagem
        byte[] buffer = mensagemTeste.toByteArray();

        // Escrevendo contato já serializado em arquivo
        FileOutputStream fos = new FileOutputStream(new File("mensagem.bin"));
        fos.write(buffer);
        fos.close();
        System.out.println("Contato escrito em formato binário no arquivo \"mensagem.bin\"");
        

        // Mapeando a mensagem para o formato json
        String json = JsonFormat.printer().print(mensagemTeste);

        // Escrita do conteúdo json em arquivo texto
        fos = new FileOutputStream(new File("mensagem.json"));
        fos.write(json.getBytes());
        fos.close();
        System.out.println("Contato escrito em formato texto/json no arquivo \"mensagem.json\"");
    }
}
