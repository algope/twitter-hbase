package org.upm.hbase.twitterhbase;


import java.io.File;


public class App {
    public static void main(String[] args) {
        TwitterHbase hbaseapp;
        System.out.println("----------------------------------------");
        System.out.println("----------------------------------------");
        System.out.println("--- Welcome to the twitter-hbase app ---");
        System.out.println("----------------------------------------");


        if (args.length > 0) {

            int mode = Integer.parseInt(args[0]);

            switch (mode) {
                case 1: {//mode ZKHOST: ZKPORT startTS endTS N language outputFolder
                    System.out.println("---Starting with parameters: -----------");
                    System.out.println("  --Mode: " + args[0]);
                    System.out.println("  --zkHost: " + args[1]);
                    System.out.println("  --startTS: " + args[2]);
                    System.out.println("  --endTS: " + args[3]);
                    System.out.println("  --N: " + args[4]);
                    System.out.println("  --Languages: " + args[5]);
                    System.out.println("  --outputFolder: " + args[6]);
                    hbaseapp = new TwitterHbase(mode, args[1], args[2], args[3], Integer.parseInt(args[4]), args[5].split(","), args[6]);
                    hbaseapp.run(mode);
                    break;
                }
                case 2: {//mode ZKHOST: ZKPORT startTS endTS N language outputFolder
                    System.out.println("---Starting with parameters: -----------");
                    System.out.println("  --Mode: " + args[0]);
                    System.out.println("  --zkHost: " + args[1]);
                    System.out.println("  --startTS: " + args[2]);
                    System.out.println("  --endTS: " + args[3]);
                    System.out.println("  --N: " + args[4]);
                    System.out.println("  --Languages: " + args[5]);
                    System.out.println("  --outputFolder: " + args[6]);
                    hbaseapp = new TwitterHbase(mode, args[1], args[2], args[3], Integer.parseInt(args[4]), args[5].split(","), args[6]);
                    hbaseapp.run(mode);
                    break;
                }
                case 3: {//mode ZKHOST: ZKPORT startTS endTS N outputFolder
                    System.out.println("---Starting with parameters: -----------");
                    System.out.println("  --Mode: " + args[0]);
                    System.out.println("  --zkHost: " + args[1]);
                    System.out.println("  --startTS: " + args[2]);
                    System.out.println("  --endTS: " + args[3]);
                    System.out.println("  --N: " + args[4]);
                    System.out.println("  --outputFolder: " + args[5]);
                    hbaseapp = new TwitterHbase(mode, args[1], args[2], args[3], Integer.parseInt(args[4]), args[5]);
                    hbaseapp.run(mode);
                    break;
                }
                case 4: {//mode ZKHOST:ZKPORT dataFolder
                    System.out.println("---Starting with parameters: -----------");
                    System.out.println("  --Mode: " + args[0]);
                    System.out.println("  --zkHost: " + args[1]);
                    System.out.println("  --dataFolder: " + args[2]);
                    File folder = new File(args[2]);
                    File[] listOfFiles = folder.listFiles();
                    assert listOfFiles != null;
                    String[] langs = new String[listOfFiles.length];
                    for (int i = 0; i < listOfFiles.length; i++) {
                        File file = listOfFiles[i];
                        if (file.isFile() && file.getName().endsWith(".out")) {
                            langs[i] = file.getName().split(".out")[0];
                        }
                    }
                    hbaseapp = new TwitterHbase(mode, args[1], args[2], langs);
                    hbaseapp.run(mode);
                    break;
                }
                default: {
                    System.out.println("[ERROR] - wrong mode");
                }
            }
        } else {
            System.out.println("[ERROR] - Expected arguments - mode dataFolder startTS endTS N language outputFolder");
            System.exit(1);
        }

    }

}