package br.ufs.dcomp.ChatRabbitMQ;

import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.protobuf.ByteString;

public class serializacaoMensagem{


	public static byte[] getSerializa(String emissor, String grupo, String texto, byte [] arquivo, String nomeArquivo, String tipoMime) throws Exception {
	        
	        String data = new SimpleDateFormat("dd/MM/yyyy").format(new Date());
	        String hora = new SimpleDateFormat("HH:mm").format(new Date());
                
                byte[] dadosParaEnviar = (arquivo != null) ? arquivo : texto.getBytes("UTF-8");
                String tipo = (tipoMime == null ? "text/plain" : tipoMime);
                String nome = (nomeArquivo == null ? "" : nomeArquivo);
	        
                MensagemProto.Conteudo conteudo = MensagemProto.Conteudo.newBuilder()
                        .setTipo(tipo)
                        .setCorpo(com.google.protobuf.ByteString.copyFrom(dadosParaEnviar))
                        .setNome(nome)
                        .build();
            
                MensagemProto.Mensagem msg = MensagemProto.Mensagem.newBuilder()
                        .setEmissor(emissor)
                        .setData(data)
                        .setHora(hora)
                        .setGrupo(grupo == null ? "" : grupo) 
                        .setConteudo(conteudo)
                        .build();
            
                return msg.toByteArray();
    }



}