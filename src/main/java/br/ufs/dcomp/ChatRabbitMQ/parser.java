package br.ufs.dcomp.ChatRabbitMQ;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class parser {
  
  public static void main(String[] argv) throws Exception {
    String baitola = "!addGroup amigos";
    String antonio = "!addUser marcio amigos";
    
    for(String b : baitola.split(" ")) {
        System.out.println(b);
    }
    
    for(String b : antonio.split(" ")) {
        System.out.println(b);
    }
    
    
    
    System.out.println();
  }
}