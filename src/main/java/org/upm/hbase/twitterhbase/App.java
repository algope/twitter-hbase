package org.upm.hbase.twitterhbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;


public class App {
    private HTable table;
    private String tableName;
    //private HashMap<String, Integer> counters;
    private int mode;
    private String zhosts;
    private String startTS;
    private String endTS;
    private int n;
    private String[] languages;
    private String dataFolder;
    private String outputFolder;

    public App(int mode, String zhosts, String startTS, String endTS, int n, String[] languages, String outputFolder) {
        this.mode = mode;
        this.zhosts = zhosts;
        this.startTS = startTS;
        this.endTS = endTS;
        this.n = n;
        this.languages = languages;
        this.outputFolder = outputFolder;
    }

    public App(int mode, String zhosts, String startTS, String endTS, int n, String outputFolder) {
        this.mode = mode;
        this.zhosts = zhosts;
        this.startTS = startTS;
        this.endTS = endTS;
        this.n = n;
        this.outputFolder = outputFolder;
    }

    public App(int mode, String zhosts, String dataFolder, String[] languages) {
        this.mode = mode;
        this.zhosts = zhosts;
        this.dataFolder = dataFolder;
        this.languages = languages;
    }

    public static void main(String[] args) {
        App hbaseapp = null;
        System.out.println("----------------------------------------");
        System.out.println("----------------------------------------");
        System.out.println("--- Welcome to the twitter-hbase app ---");
        System.out.println("----------------------------------------");
        System.out.println("");


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
                    hbaseapp = new App(mode, args[1], args[2], args[3], Integer.parseInt(args[4]), args[5].split(","), args[6]);
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
                    hbaseapp = new App(mode, args[1], args[2], args[3], Integer.parseInt(args[4]), args[5].split(","), args[6]);
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
                    hbaseapp = new App(mode, args[1], args[2], args[3], Integer.parseInt(args[4]), args[5]);
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
                    hbaseapp = new App(mode, args[1], args[2], langs);
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


    private void run(int mode) {
        setTableName("twitterStats");
        System.setProperty("hadoop.home.dir", "/");
        Configuration conf = HBaseConfiguration.create();
        String[] hostip = getZhosts().split(":");
        conf.set("hbase.zookeeper.quorum", hostip[0]);
        conf.set("hbase.zookeeper.property.clientPort", hostip[1]);
        HBaseAdmin admin;
        System.out.println("[INFO] - Connected to HBase server");
        try {
            admin = new HBaseAdmin(conf);
            if (!admin.tableExists(TableName.valueOf(getTableName()))) {
                System.out.println("[INFO] - Creating table in HBase");

                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(getTableName()));
                for (String language : languages) {
                    System.out.println("[INFO] - Creating column family for language: " + language);
                    tableDescriptor.addFamily(new HColumnDescriptor(language));
                }
                admin.createTable(tableDescriptor);
                HConnection conn = HConnectionManager.createConnection(conf);
                setTable(new HTable(TableName.valueOf(getTableName()), conn));
                System.out.println("[INFO] - HBase table created");
            } else {
                System.out.println("[INFO] - Connected to existing table");
                HConnection conn = HConnectionManager.createConnection(conf);
                setTable(new HTable(TableName.valueOf(getTableName()), conn));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        switch (mode) {
            case 1: {
                HashMap<String, Integer> counters = new HashMap<>();
                Scan scan = new Scan(generateKey(getStartTS()), generateKey(getEndTS()));
                String lang = getLanguages()[0];
                scan.addFamily(Bytes.toBytes(lang));
                ResultScanner rs;
                try {
                    rs = getTable().getScanner(scan);
                    Result res = rs.next();
                    while (res != null && !res.isEmpty()) {
                        byte[] word = res.getValue(Bytes.toBytes(lang), Bytes.toBytes("WORD"));
                        byte[] frequency = res.getValue(Bytes.toBytes(lang), Bytes.toBytes("FREQUENCY"));
                        String wordString = Bytes.toString(word);
                        String countString = Bytes.toString(frequency);
                        if (counters.containsKey(wordString)) {
                            int freq = counters.get(wordString);
                            freq = freq + Integer.parseInt(countString);
                            counters.put(wordString, freq);
                        } else {
                            counters.put(wordString, Integer.parseInt(countString));
                        }

                        res = rs.next();
                    }
                    Set<Entry<String, Integer>> set = counters.entrySet();
                    List<Entry<String, Integer>> list = new ArrayList<>(set);
                    Collections.sort(list, new HashtagComparator());
                    int position = 1;
                    for (Map.Entry<String, Integer> entry : list) {
                        File file = new File(getOutputFolder() + "/09_" + "query1" + ".out");
                        String line;
                        line = lang + ", " + position + ", " + entry.getKey() + ", " + getStartTS() + ", " + getEndTS();

                        System.out.println(">>>>>> ENTRY >>>>>>>> WORD: " + entry.getKey() + "  COUNT: " + entry.getValue());

                        BufferedWriter bw;
                        try {
                            bw = new BufferedWriter(new FileWriter(file, true));
                            bw.append(line);
                            bw.newLine();
                            bw.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        if (position == getN())
                            break;
                        else
                            position++;
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("[INFO] - Done!");
                System.exit(0);
                break;
            }
            case 2: {
                HashMap<String, Integer> counters = new HashMap<>();
                for (int i = 0; i <= getLanguages().length - 1; i++) {
                    try {
                        if (getTable().getTableDescriptor().hasFamily(Bytes.toBytes(languages[i]))) {
                            //executeQuery("query2", start_timestamp, end_timestamp, N, languages[i], outputFolderPath);
                            Scan scan = new Scan(generateKey(getStartTS()), generateKey(getEndTS()));
                            scan.addFamily(Bytes.toBytes(getLanguages()[i]));
                            ResultScanner rs;
                            try {
                                rs = getTable().getScanner(scan);
                                Result res = rs.next();

                                while (res != null && !res.isEmpty()) {
                                    byte[] word = res.getValue(Bytes.toBytes(getLanguages()[i]), Bytes.toBytes("WORD"));
                                    byte[] frequency = res.getValue(Bytes.toBytes(getLanguages()[i]), Bytes.toBytes("FREQUENCY"));
                                    String wordString = Bytes.toString(word);
                                    String countString = Bytes.toString(frequency);


                                    if (counters.containsKey(wordString)) {
                                        int freq = counters.get(wordString);
                                        freq = freq + Integer.parseInt(countString);
                                        counters.put(wordString, freq);
                                    } else {
                                        counters.put(wordString, Integer.parseInt(countString));
                                    }


                                    res = rs.next();
                                }
                                Set<Entry<String, Integer>> set = counters.entrySet();
                                List<Entry<String, Integer>> list = new ArrayList<>(set);
                                Collections.sort(list, new HashtagComparator());
                                int position = 1;
                                for (Map.Entry<String, Integer> entry : list) {
                                    File file = new File(getOutputFolder() + "/09_" + "query2" + ".out");
                                    String line;
                                    line = getLanguages()[i] + ", " + position + ", " + entry.getKey() + ", " + getStartTS() + ", " + getEndTS();

                                    System.out.println(">>>>>> ENTRY >>>>>>>> WORD: " + entry.getKey() + "  COUNT: " + entry.getValue());

                                    BufferedWriter bw;
                                    try {
                                        bw = new BufferedWriter(new FileWriter(file, true));
                                        bw.append(line);
                                        bw.newLine();
                                        bw.close();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                    if (position == getN())
                                        break;
                                    else
                                        position++;
                                }

                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                break;
            }
            case 3: {
                String[] query_languages;
                try {
                    query_languages = new String[getTable().getTableDescriptor().getColumnFamilies().length];
                    for (int i = 0; i <= getTable().getTableDescriptor().getColumnFamilies().length - 1; i++) {
                        query_languages[i] = getTable().getTableDescriptor().getColumnFamilies()[i].getNameAsString();
                        //executeQuery("query3", start_timestamp, end_timestamp, N, query_languages[i], outputFolderPath);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                //arrangeAndPrint(getCounters(), "query3", null, start_timestamp, end_timestamp, outputFolderPath, N);
                break;
            }
            case 4: {
                System.out.println("[INFO] - Loading the data from: " + getDataFolder() + " into HBase");
                File folder = new File(dataFolder);
                File[] listOfFiles = folder.listFiles();

                assert listOfFiles != null;
                for (File file : listOfFiles)
                    if (file.isFile() && file.getName().endsWith(".out")) {
                        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                String[] splitLine = line.split(",");
                                String timestamp = splitLine[0];
                                String lang = splitLine[1];

                                int seek = 2;
                                int rank = 1;

                                while (seek < splitLine.length) {
                                    String word = splitLine[seek++];
                                    String freq = splitLine[seek++];
                                    byte[] key = generateKey(timestamp, lang, Integer.toString(rank));
                                    Get get = new Get(key);
                                    Result res;
                                    try {
                                        res = getTable().get(get);
                                        if (res != null) {
                                            Put put = new Put(key);
                                            //columnFamily, column, value
                                            put.add(Bytes.toBytes(lang), Bytes.toBytes("WORD"), Bytes.toBytes(word));
                                            put.add(Bytes.toBytes(lang), Bytes.toBytes("FREQUENCY"), Bytes.toBytes(freq));
                                            System.out.println("[INFO] - Adding to: " + lang + " -WORD-" + word + " -FREQUENCY-" + freq);
                                            getTable().put(put);
                                        }
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                    rank++;
                                }

                            }

                        } catch (NumberFormatException | IOException e) {
                            e.printStackTrace();
                        }
                    }

                System.out.println("[INFO] - Done!");
                System.exit(0);
                break;
            }
        }


    }

    /**
     * Method to generate the structure of the key
     */
    private byte[] generateKey(String timestamp) {
        byte[] key = new byte[44];
        System.arraycopy(Bytes.toBytes(timestamp), 0, key, 0, timestamp.length());
        return key;
    }

    /**
     * Method to generate the structure of the key
     */
    private static byte[] generateKey(String timestamp, String lang, String topic_pos) {
        byte[] key = new byte[44];
        System.arraycopy(Bytes.toBytes(timestamp), 0, key, 0, timestamp.length());
        System.arraycopy(Bytes.toBytes(lang), 0, key, 20, lang.length());
        System.arraycopy(Bytes.toBytes(topic_pos), 0, key, 20, topic_pos.length());
        return key;
    }


//    /**
//     * Method to arrange and print the query result
//     */
//    private static void arrangeAndPrint(HashMap<String, Integer> intervalTopTopic, String query, String lang, String start_timestamp, String end_timestamp, String output_folder, int N) {
//        // Process the results and print them
//
//    }

//    private void executeQuery(String query, String start_timestamp, String end_timestamp, int N, String[] lang, String out_folder_path) {
//        System.out.println("Executing the " + query);
//
//        Scan scan = new Scan(generateKey(start_timestamp), generateKey(end_timestamp));
//        scan.addFamily(Bytes.toBytes(lang));
//        ResultScanner rs;
//        try {
//            rs = getTable().getScanner(scan);
//            Result res = rs.next();
//            if (!query.equals("query3"))
//                setCounters(new HashMap<String, Long>());
//            while (res != null && !res.isEmpty()) {
//                byte[] word = res.getValue(Bytes.toBytes(lang), Bytes.toBytes("WORD"));
//                byte[] frequency = res.getValue(Bytes.toBytes(lang), Bytes.toBytes("FREQUENCY"));
//                String wordString = Bytes.toString(word);
//                String countString = Bytes.toString(frequency);
//                getCounters().put(wordString, (long) Integer.parseInt(countString));
//                res = rs.next();
//            }
//            if (!query.equals("query3"))
//                arrangeAndPrint(getCounters(), query, lang, start_timestamp, end_timestamp, out_folder_path, N);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }


    /**
     * Method to perform the second query
     * Do find the list of Top-N most used words for each language in a time interval defined
     * with the provided start and end timestamp. Start and end timestamp are in milliseconds.
     */
//    private void secondQuery(String start_timestamp, String end_timestamp, int N, String[] languages, String outputFolderPath) {
//
//
//    }

    /**
     * Method to perform the third query
     * Do find the Top-N most used words and the frequency of each word regardless the
     * language in a time interval defined with the provided start and end timestamp. Start
     * and end timestamp are in milliseconds.
     */
//    private void thirdQuery(String start_timestamp, String end_timestamp, int N, String outputFolderPath) {
//        setCounters(new HashMap<String, Long>());
//        String[] query_languages;
//        try {
//            query_languages = new String[getTable().getTableDescriptor().getColumnFamilies().length];
//            for (int i = 0; i <= getTable().getTableDescriptor().getColumnFamilies().length - 1; i++) {
//                query_languages[i] = getTable().getTableDescriptor().getColumnFamilies()[i].getNameAsString();
//                executeQuery("query3", start_timestamp, end_timestamp, N, query_languages[i], outputFolderPath);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        arrangeAndPrint(getCounters(), "query3", null, start_timestamp, end_timestamp, outputFolderPath, N);
//    }

    /**
     * Method to store the results of the query in a file
     */
//    private static void writeInOutputFile(String query, String language, int position, String word, String startTS, String endTS, String out_folder_path, String frecuency) {
//        File file = new File(out_folder_path + "/09_" + query + ".out");
//        String content;
//        if (query.equals("query3"))
//            content = position + ", " + word + ", " + frecuency + ", " + startTS + ", " + endTS;
//        else
//            content = language + ", " + position + ", " + word + ", " + startTS + ", " + endTS;
//
//        BufferedWriter bw;
//        try {
//            bw = new BufferedWriter(new FileWriter(file, true));
//            bw.append(content);
//            bw.newLine();
//            bw.close();
//        } catch (IOException ex) {
//            System.out.println(ex.getMessage());
//        }
//    }
    private HTable getTable() {
        return table;
    }

    private void setTable(HTable table) {
        this.table = table;
    }

//    private HashMap<String, Integer> getCounters() {
//        return counters;
//    }

//    private void setCounters(HashMap<String, Integer> counters) {
//        this.counters = counters;
//    }

    public int getMode() {
        return mode;
    }

    public void setMode(int mode) {
        this.mode = mode;
    }

    public String getZhosts() {
        return zhosts;
    }

    public void setZhosts(String zhosts) {
        this.zhosts = zhosts;
    }

    public String getStartTS() {
        return startTS;
    }

    public void setStartTS(String startTS) {
        this.startTS = startTS;
    }

    public String getEndTS() {
        return endTS;
    }

    public void setEndTS(String endTS) {
        this.endTS = endTS;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public String[] getLanguages() {
        return languages;
    }

    public void setLanguages(String[] languages) {
        this.languages = languages;
    }

    public String getDataFolder() {
        return dataFolder;
    }

    public void setDataFolder(String dataFolder) {
        this.dataFolder = dataFolder;
    }

    public String getOutputFolder() {
        return outputFolder;
    }

    public void setOutputFolder(String outputFolder) {
        this.outputFolder = outputFolder;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

}


//   HashMap<String, Integer> hashmap = new HashMap<>();
//                            String key = splitLine[seek];
//                            if(hashmap.containsKey(key)){
//                                seek++;
//                               int freq = hashmap.get(splitLine[seek]);
//                               freq = freq + Integer.parseInt(splitLine[seek]);
//                               hashmap.put(key, freq);
//                            }
//                            else{
//                                seek++;
//                                hashmap.put(key, Integer.parseInt(splitLine[seek]));
//                            }
//                            seek++;