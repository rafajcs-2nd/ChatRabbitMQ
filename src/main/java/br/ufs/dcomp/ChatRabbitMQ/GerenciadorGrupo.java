package br.ufs.dcomp.ChatRabbitMQ;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.List;

public class GerenciadorGrupo {

    // --- CONFIGURAÇÕES DO CLOUDAMQP ---
    // Preencha com os dados do painel do CloudAMQP (Detalhes)
    
    private static final String HOST_API = "shark.rmq.cloudamqp.com"; // Sem https://, só o domínio
    private static final String VHOST    = "qynhcgcj"; // No CloudAMQP, geralmente é igual ao User
    private static final String USER     = "qynhcgcj";
    private static final String PASS     = "PpmRlIuA8oSOCN2wHCSP4R9dJf99WIJU"; 

    // Método genérico para chamar a API
    private static String chamarAPI(String endpoint) {
        try {
            // CloudAMQP usa HTTPS na porta padrão (443), não precisa indicar porta
            String urlString = "https://" + HOST_API + "/api/" + endpoint;
            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            
            conn.setRequestMethod("GET");
            
            // Autenticação Basic Auth
            String auth = USER + ":" + PASS;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
            conn.setRequestProperty("Authorization", "Basic " + encodedAuth);

            if (conn.getResponseCode() != 200) {
                // Se der erro (ex: 404), retorna null
                return null;
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
            return response.toString();
        } catch (Exception e) {
            System.out.println("[Erro API] " + e.getMessage());
            return null;
        }
    }

    // !listGroups
    public static List<String> listarGruposDoUsuario(String usuario) {
        List<String> grupos = new ArrayList<>();
        // URL: /queues/{vhost}/{queue_name}/bindings
        String json = chamarAPI("queues/" + VHOST + "/" + usuario + "/bindings");
        
        if (json != null) {
            // Regex para pegar o campo "source" (que é o nome do exchange/grupo)
            Pattern pattern = Pattern.compile("\"source\":\"([^\"]+)\"");
            Matcher matcher = pattern.matcher(json);
            
            while (matcher.find()) {
                String grupo = matcher.group(1);
                // Filtra exchanges internos ou vazios
                if (!grupo.isEmpty() && !grupo.equals("usuarios_direct") && !grupo.startsWith("amq.")) {
                    grupos.add(grupo);
                }
            }
        }
        return grupos;
    }

    // !listUsers
    public static List<String> listarUsuariosDoGrupo(String grupo) {
        List<String> usuarios = new ArrayList<>();
        // URL: /exchanges/{vhost}/{exchange_name}/bindings/source
        String json = chamarAPI("exchanges/" + VHOST + "/" + grupo + "/bindings/source");
        
        if (json != null) {
            // Regex para pegar o campo "destination" (que é o nome da fila/usuário)
            Pattern pattern = Pattern.compile("\"destination\":\"([^\"]+)\"");
            Matcher matcher = pattern.matcher(json);
            
            while (matcher.find()) {
                String user = matcher.group(1);
                usuarios.add(user);
            }
        }
        return usuarios;
    }
}