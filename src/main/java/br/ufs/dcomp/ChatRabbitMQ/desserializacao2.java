public class desserializacao2{

    public String desserializacao(String nomeArquivo){

        File file = new File(nomeArquivo);
        FileInputStream fis = new FileInputStream(file);
        byte[] buffer = new byte[(int) file.length()];
        fis.read(buffer);
        fis.close();

        if(nomeArquivo == "msgGrupo"){

            // Mapeando bytes para a mensagem protobuf
            ContatoProto.mensagemGrupo gp = ContatoProto.mensagemGrupo.parseFrom(buffer);

            String dataHora = mensagemGrupo.getDataHora();
            String mensagem = mensagemGrupo.getMsg();
            System.out.println("(" + dataHora + ") @" + mensagem);

            return "(" + dataHora + ") @" + mensagem;
        }

        else{

            ContatoProto.mensagemUsuario user = ContatoProto.mensagemUsuario.parseFrom(buffer);

            String dataHora = mensagemUsuario.getDataHora();
            String mensagem = mensagemUsuario.getMsg();
            return "(" + dataHora + ") @" + mensagem;
        }

    }

}
