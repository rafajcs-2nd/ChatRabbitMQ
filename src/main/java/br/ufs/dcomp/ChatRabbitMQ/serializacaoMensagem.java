public class serializacaoMenssagem{


	public FileOutputStream getSerializaGrupo(int codigoEnvio, String mensagemRecebida, String nomeGrupo, String nomeRemetente, String nomeDestinatario, String dataHorario){
			ContatoProto.mensagemGrupo grupo = ContatoProto.mensagemGrupo.newBuilder()
			.setMsg(mensagemRecebida)
			.setNomeGrupo(nomeGrupo)
			.setNomeRemetente(nomeRemetente)
			.setNomeDestinatario(nomeDestinatario)
			.setDataHora(dataHorario).build();


            byte[] buffer = mensagemGrupo.toByteArray();

        // Escrevendo contato já serializado em arquivo
            FileOutputStream fos = new FileOutputStream(new File("msgGrupo.bin"));
            fos.write(buffer);
            fos.close();
            //System.out.println("Contato escrito em formato binário no arquivo \"aluno.bin\"");

            // Mapeando a mensagem para o formato json
            String json = JsonFormat.printer().print(mensagemGrupo);

            // Escrita do conteúdo json em arquivo texto
            fos = new FileOutputStream(new File("msgGrupo.json"));
            fos.write(json.getBytes());
            fos.close();
            //System.out.println("Contato escrito em formato texto/json no arquivo \"msgGrupo.json\"");

            return fos;
	}

    public String getSerializaUsuario(int codigoEnvio, String mensagemRecebida, String nomeRemetente, String nomeDestinatario, String dataHorario){
			ContatoProto.mensagemUsuario usuario = ContatoProto.mensagemUsuario.newBuilder()
			.setMsg(mensagemRecebida)
			.setNomeRemetente(nomeRemetente)
			.setNomeDestinatario(nomeDestinatario)
			.setDataHora(dataHorario).build();build();


            byte[] buffer = mensagemGrupo.toByteArray();

        // Escrevendo contato já serializado em arquivo
            FileOutputStream fos = new FileOutputStream(new File("msgUsuario.bin"));
            fos.write(buffer);
            fos.close();
            //System.out.println("Contato escrito em formato binário no arquivo \"aluno.bin\"");

            // Mapeando a mensagem para o formato json
            String json = JsonFormat.printer().print(mensagemUsuario);

            // Escrita do conteúdo json em arquivo texto
            fos = new FileOutputStream(new File("msgUsuario.json"));
            fos.write(json.getBytes());
            fos.close();
            //System.out.println("Contato escrito em formato texto/json no arquivo \"msgGrupo.json\"");

            return fos;
	}


}