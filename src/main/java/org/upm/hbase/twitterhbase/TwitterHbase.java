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

public class TwitterHbase {

    private HTable table;
    private String tableName;
    private int mode;
    private String zhosts;
    private String startTS;
    private String endTS;
    private int n;
    private String[] languages;
    private String dataFolder;
    private String outputFolder;

    public TwitterHbase(int mode, String zhosts, String startTS, String endTS, int n, String[] languages, String outputFolder) {
        this.mode = mode;
        this.zhosts = zhosts;
        this.startTS = startTS;
        this.endTS = endTS;
        this.n = n;
        this.languages = languages;
        this.outputFolder = outputFolder;
    }

    public TwitterHbase(int mode, String zhosts, String startTS, String endTS, int n, String outputFolder) {
        this.mode = mode;
        this.zhosts = zhosts;
        this.startTS = startTS;
        this.endTS = endTS;
        this.n = n;
        this.outputFolder = outputFolder;
    }

    public TwitterHbase(int mode, String zhosts, String dataFolder, String[] languages) {
        this.mode = mode;
        this.zhosts = zhosts;
        this.dataFolder = dataFolder;
        this.languages = languages;
    }


    public void run(int mode) {
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
                System.out.println("[INFO] - Running first query");
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
                    Set<Map.Entry<String, Integer>> set = counters.entrySet();
                    List<Map.Entry<String, Integer>> list = new ArrayList<>(set);
                    Collections.sort(list, new HashtagComparator());
                    int position = 1;
                    for (Map.Entry<String, Integer> entry : list) {
                        File file = new File(getOutputFolder() + "/09_query1.out");
                        String line = lang + ", " + position + ", " + entry.getKey() + ", " + getStartTS() + ", " + getEndTS();
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
                break;
            }
            case 2: {
                System.out.println("[INFO] - Running second query");
                HashMap<String, Integer> counters = new HashMap<>();
                for (int i = 0; i <= getLanguages().length - 1; i++) {
                    try {
                        if (getTable().getTableDescriptor().hasFamily(Bytes.toBytes(languages[i]))) {
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
                                Set<Map.Entry<String, Integer>> set = counters.entrySet();
                                List<Map.Entry<String, Integer>> list = new ArrayList<>(set);
                                Collections.sort(list, new HashtagComparator());
                                int position = 1;
                                for (Map.Entry<String, Integer> entry : list) {
                                    File file = new File(getOutputFolder() + "/09_query2.out");
                                    String line = getLanguages()[i] + ", " + position + ", " + entry.getKey() + ", " + getStartTS() + ", " + getEndTS();

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
                System.out.println("[INFO] - Running third query");
                HashMap<String, Integer> counters = new HashMap<>();
                String[] colFam;
                try {
                    colFam = new String[getTable().getTableDescriptor().getColumnFamilies().length];
                    for (int i = 0; i <= getTable().getTableDescriptor().getColumnFamilies().length - 1; i++) {
                        colFam[i] = getTable().getTableDescriptor().getColumnFamilies()[i].getNameAsString();
                        Scan scan = new Scan(generateKey(getStartTS()), generateKey(getEndTS()));
                        scan.addFamily(Bytes.toBytes(colFam[i]));
                        ResultScanner rs;
                        try {
                            rs = getTable().getScanner(scan);
                            Result res = rs.next();
                            while (res != null && !res.isEmpty()) {
                                byte[] word = res.getValue(Bytes.toBytes(colFam[i]), Bytes.toBytes("WORD"));
                                byte[] frequency = res.getValue(Bytes.toBytes(colFam[i]), Bytes.toBytes("FREQUENCY"));
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

                        } catch (IOException e) {
                            e.printStackTrace();
                        }


                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Set<Map.Entry<String, Integer>> set = counters.entrySet();
                List<Map.Entry<String, Integer>> list = new ArrayList<>(set);
                Collections.sort(list, new HashtagComparator());
                int position = 1;
                for (Map.Entry<String, Integer> entry : list) {
                    File file = new File(getOutputFolder() + "/09_query3.out");
                    String line = position + ", " + entry.getValue() + ", " + entry.getKey() + ", " + getStartTS() + ", " + getEndTS();
                    BufferedWriter bw;
                    try {
                        bw = new BufferedWriter(new FileWriter(file, true));
                        bw.append(line);
                        bw.newLine();
                        bw.close();
                    } catch (IOException ex) {
                        System.out.println(ex.getMessage());
                    }
                    if (position == getN())
                        break;
                    else
                        position++;
                }


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
                break;
            }
        }
        System.out.println("[INFO] - Done!");
        System.exit(0);


    }

    public byte[] generateKey(String timestamp) {
        byte[] key = new byte[44];
        System.arraycopy(Bytes.toBytes(timestamp), 0, key, 0, timestamp.length());
        return key;
    }

    public byte[] generateKey(String timestamp, String lang, String topic_pos) {
        byte[] key = new byte[44];
        System.arraycopy(Bytes.toBytes(timestamp), 0, key, 0, timestamp.length());
        System.arraycopy(Bytes.toBytes(lang), 0, key, 20, lang.length());
        System.arraycopy(Bytes.toBytes(topic_pos), 0, key, 20, topic_pos.length());
        return key;
    }


    public HTable getTable() {
        return table;
    }

    public void setTable(HTable table) {
        this.table = table;
    }

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
