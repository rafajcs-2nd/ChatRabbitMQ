package br.ufs.dcomp.ChatRabbitMQ;

import java.io.FileInputStream;
import java.io.File;
import com.google.protobuf.util.JsonFormat;

public class Desserializacao {
    public static void main(String[] args) throws Exception {
        // Arquivo binário gerado na serialização
        File file = new File("mensagem.bin");
        FileInputStream fis = new FileInputStream(file);
        byte[] buffer = new byte[(int) file.length()];
        fis.read(buffer);
        fis.close();

        // Mapeando bytes para a mensagem protobuf
        MensagemProto.Mensagem mensagemLida = MensagemProto.Mensagem.parseFrom(buffer);

        // Extraindo campos principais
        String emissor = mensagemLida.getEmissor();
        String data = mensagemLida.getData();
        String hora = mensagemLida.getHora();
        String grupo = mensagemLida.getGrupo();

        System.out.println("=== Dados da Mensagem Desserializada ===");
        System.out.println("Emissor: " + emissor);
        System.out.println("Data: " + data);
        System.out.println("Hora: " + hora);
        System.out.println("Grupo: " + grupo);

        // Extraindo o conteúdo (submensagem)
        MensagemProto.Conteudo conteudo = mensagemLida.getConteudo();
        System.out.println("\n--- Conteúdo ---");
        System.out.println("Tipo: " + conteudo.getTipo());
        System.out.println("Nome: " + conteudo.getNome());
        System.out.println("Tamanho (bytes): " + conteudo.getCorpo().size());

        // Exibindo como JSON (opcional)
        String json = JsonFormat.printer().print(mensagemLida);
        System.out.println("\nRepresentação JSON:\n" + json);
    }
}
