public class serializacaoMenssagem{


	public static FileOutputStream getSerializaGrupo(String emissor, String grupo, String texto) throws Exception {

    MensagemProto.Conteudo conteudo = MensagemProto.Conteudo.newBuilder()
            .setTipo("text/plain")
            .setCorpo(ByteString.copyFrom(texto.getBytes()))
            .setNome("mensagem.txt")
            .build();

    MensagemProto.Mensagem msg = MensagemProto.Mensagem.newBuilder()
            .setEmissor(emissor)
            .setData("hoje")
            .setHora("agora")
            .setGrupo(grupo)
            .setConteudo(conteudo)
            .build();

    byte[] buffer = msg.toByteArray();

    FileOutputStream fos = new FileOutputStream(new File("msg.bin"));
    fos.write(buffer);
    fos.close();

        return fos;
    }



}