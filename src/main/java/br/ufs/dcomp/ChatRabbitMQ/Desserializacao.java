package br.ufs.dcomp.ChatRabbitMQ;

import java.io.FileInputStream;
import java.io.File;
import com.google.protobuf.util.JsonFormat;

public class Desserializacao {

    public static String returnMsgDess(String nomeArqBin) throws Exception {

        
        File file = new File(nomeArqBin);
        FileInputStream fis = new FileInputStream(file);
        byte[] buffer = new byte[(int) file.length()];
        fis.read(buffer);
        fis.close();

        
        MensagemProto.Mensagem mensagemLida = MensagemProto.Mensagem.parseFrom(buffer);

        
        String emissor = mensagemLida.getEmissor();
        String data = mensagemLida.getData();
        String hora = mensagemLida.getHora();
        String grupo = mensagemLida.getGrupo();

        MensagemProto.Conteudo conteudo = mensagemLida.getConteudo();
        
        String tipo = conteudo.getTipo();
        String nome = conteudo.getNome();
        int tamanho = conteudo.getCorpo().size();

        
        String json = JsonFormat.printer().print(mensagemLida);

        
        String resultado = emissor + " " + data + " " + hora + " " + grupo + " " + tipo + " " + nome + " " + tamanho + " " + json;


        return resultado;
    }
}
