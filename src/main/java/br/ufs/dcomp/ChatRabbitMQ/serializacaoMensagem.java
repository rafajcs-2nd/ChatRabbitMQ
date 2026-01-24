package br.ufs.dcomp.ChatRabbitMQ;

import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.protobuf.ByteString;

public class serializacaoMensagem{


	public static byte[] getSerializaGrupo(String emissor, String grupo, String texto) throws Exception {
	        
	        String data = new SimpleDateFormat("dd/MM/yyyy").format(new Date());
	        String hora = new SimpleDateFormat("HH:mm").format(new Date());
	        
                MensagemProto.Conteudo conteudo = MensagemProto.Conteudo.newBuilder()
                        .setTipo("text/plain")
                        .setCorpo(ByteString.copyFrom(texto.getBytes()))
                        .setNome("")
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