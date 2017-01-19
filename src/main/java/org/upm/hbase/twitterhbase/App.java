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
    private Map<String, Long> intervalTopTopic;

    public void main(String[] args) {
        if (args.length > 0) {
            try {
                int mode = Integer.parseInt(args[0]);
                if (mode == 4)
                    getTable(extractLangsSource(args[1]));
                switch (mode) {
                    case 1:
                        firstQuery(args[1], args[2], Integer.parseInt(args[3]), args[4], args[5]);
                        break;
                    case 2:
                        secondQuery(args[1], args[2], Integer.parseInt(args[3]), args[4].split(","), args[5]);
                        break;
                    case 3:
                        thirdQuery(args[1], args[2], Integer.parseInt(args[3]), args[4]);
                        break;
                    case 4:
                        load(args[1]);
                        break;
                }


            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

        } else {
            System.out.println("[ERROR] - Expected arguments - mode dataFolder startTS endTS N language outputFolder");
            System.exit(1);
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
    private byte[] generateKey(String timestamp, String lang, String topic_pos) {
        byte[] key = new byte[44];
        System.arraycopy(Bytes.toBytes(timestamp), 0, key, 0, timestamp.length());
        System.arraycopy(Bytes.toBytes(lang), 0, key, 20, lang.length());
        System.arraycopy(Bytes.toBytes(topic_pos), 0, key, 20, topic_pos.length());
        return key;
    }



    /**
     * Method to arrange and print the query result
     */
    private void arrangeAndPrint(Map<String, Long> intervalTopTopic, String query, String lang, String start_timestamp, String end_timestamp, String output_folder, int N) {
        // Process the results and print them
        Set<Entry<String, Long>> set = intervalTopTopic.entrySet();
        List<Entry<String, Long>> list = new ArrayList<>(set);
        Collections.sort(list, new HashtagComparator());
        int position = 1;
        for (Map.Entry<String, Long> entry : list) {
            writeInOutputFile(query, lang, position, entry.getKey(), start_timestamp, end_timestamp, output_folder, entry.getValue().toString());
            if (position == N)
                break;
            else
                position++;
        }
    }

    private void executeQuery(String query, String start_timestamp, String end_timestamp, int N, String lang, String out_folder_path) {
        System.out.println("Executing the " + query);

        Scan scan = new Scan(generateKey(start_timestamp), generateKey(end_timestamp));
        scan.addFamily(Bytes.toBytes(lang));
        ResultScanner rs;
        try {
            rs = getTable().getScanner(scan);
            Result res = rs.next();
            if (!query.equals("query3"))
                setIntervalTopTopic(new HashMap<String, Long>());
            while (res != null && !res.isEmpty()) {
                byte[] topic_bytes = res.getValue(Bytes.toBytes(lang), Bytes.toBytes("TOPIC"));
                byte[] count_bytes = res.getValue(Bytes.toBytes(lang), Bytes.toBytes("COUNTS"));
                String topic = Bytes.toString(topic_bytes);
                String count = Bytes.toString(count_bytes);
                getIntervalTopTopic().put(topic, (long) Integer.parseInt(count));
                res = rs.next();
            }
            if (!query.equals("query3"))
                arrangeAndPrint(getIntervalTopTopic(), query, lang, start_timestamp, end_timestamp, out_folder_path, N);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to perform the first query
     * Given a language (lang), do find the Top-N most used words for the given language in
     * a time interval defined with a start and end timestamp. Start and end timestamp are
     * in milliseconds.
     */
    private void firstQuery(String start_timestamp, String end_timestamp, int N, String language, String outputFolderPath) {
        executeQuery("query1", start_timestamp, end_timestamp, N, language, outputFolderPath);
    }

    /**
     * Method to perform the second query
     * Do find the list of Top-N most used words for each language in a time interval defined
     * with the provided start and end timestamp. Start and end timestamp are in milliseconds.
     */
    private void secondQuery(String start_timestamp, String end_timestamp, int N, String[] languages, String outputFolderPath) {
        for (int i = 0; i <= languages.length - 1; i++) {
            try {
                if (getTable().getTableDescriptor().hasFamily(Bytes.toBytes(languages[i]))) {
                    executeQuery("query2", start_timestamp, end_timestamp, N, languages[i], outputFolderPath);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Method to perform the third query
     * Do find the Top-N most used words and the frequency of each word regardless the
     * language in a time interval defined with the provided start and end timestamp. Start
     * and end timestamp are in milliseconds.
     */
    private void thirdQuery(String start_timestamp, String end_timestamp, int N, String outputFolderPath) {
        setIntervalTopTopic(new HashMap<String, Long>());
        String[] query_languages;
        try {
            query_languages = new String[getTable().getTableDescriptor().getColumnFamilies().length];
            for (int i = 0; i <= getTable().getTableDescriptor().getColumnFamilies().length - 1; i++) {
                query_languages[i] = getTable().getTableDescriptor().getColumnFamilies()[i].getNameAsString();
                executeQuery("query3", start_timestamp, end_timestamp, N, query_languages[i], outputFolderPath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        arrangeAndPrint(getIntervalTopTopic(), "query3", null, start_timestamp, end_timestamp, outputFolderPath, N);
    }

    /**
     * Method to extract languages from the data source
     */
    private String[] extractLangsSource(String dataFolder) {
        File folder = new File(dataFolder);
        File[] listOfFiles = folder.listFiles();
        assert listOfFiles != null;
        String[] langs = new String[listOfFiles.length];
        for (int i = 0; i < listOfFiles.length; i++) {
            File file;
            file = listOfFiles[i];
            if (file.isFile() && file.getName().endsWith(".out")) {
                langs[i] = file.getName().split(".out")[0];
            }
        }
        return langs;
    }

    /**
     * Method to create the table in hbase
     */
    private void getTable(String[] languages) {
        System.setProperty("hadoop.home.dir", "/");
        Configuration conf = HBaseConfiguration.create(); // Instantiating configuration class
        conf.set("hbase.zookeeper.quorum", "node2");
        HBaseAdmin admin;
        String thisTable = "ttt";
        try {
            admin = new HBaseAdmin(conf);
            if (!admin.tableExists(thisTable)) {// Execute the table through admin
                // Instantiating table descriptor class
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(thisTable));
                // Adding column families to table descriptor
                for (String language : languages) {
                    tableDescriptor.addFamily(new HColumnDescriptor(language));
                }
                admin.createTable(tableDescriptor);
                HConnection conn = HConnectionManager.createConnection(conf);
                setTable(new HTable(TableName.valueOf(thisTable), conn));
            } else {
                HConnection conn = HConnectionManager.createConnection(conf);
                setTable(new HTable(TableName.valueOf(thisTable), conn));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to insert rows into the hbase table
     */
    private void insertIntoTable(String timestamp, String lang, String hashtag, String counts, int topic_pos) {
        byte[] key = generateKey(timestamp, lang, Integer.toString(topic_pos));
        Get get = new Get(key);
        Result res;
        try {
            res = getTable().get(get);
            if (res != null) { // insert in table
                Put put = new Put(key);
                put.add(Bytes.toBytes(lang), Bytes.toBytes("TOPIC"), Bytes.toBytes(hashtag));
                put.add(Bytes.toBytes(lang), Bytes.toBytes("COUNTS"), Bytes.toBytes(counts));
                getTable().put(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to load the files in Hbase
     */
    private void load(String dataFolder) {
        File folder = new File(dataFolder);
        File[] listOfFiles = folder.listFiles();
        assert listOfFiles != null;
        for (File file : listOfFiles)
            if (file.isFile() && file.getName().endsWith(".out")) {

                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    for (String line; (line = br.readLine()) != null; ) {
                        // process line by line
                        String[] fields = line.split(",");
                        String timestamp = fields[0];
                        String lang = fields[1];
                        int pos = 2;
                        int topic_pos = 1;
                        while (pos < fields.length) {
                            insertIntoTable(timestamp, lang, fields[pos++], fields[pos++], topic_pos);
                            topic_pos++;
                        }
                    }
                } catch (NumberFormatException | IOException e) {
                    e.printStackTrace();
                }
            }

    }

    /**
     * Method to store the results of the query in a file
     */
    private void writeInOutputFile(String query, String language, int position, String word, String startTS, String endTS, String out_folder_path, String frecuency) {
        File file = new File(out_folder_path + "/09_" + query + ".out");
        String content;
        if (query.equals("query3"))
            content = position + ", " + word + ", " + frecuency + ", " + startTS + ", " + endTS;
        else
            content = language + ", " + position + ", " + word + ", " + startTS + ", " + endTS;

        BufferedWriter bw;
        try {
            bw = new BufferedWriter(new FileWriter(file, true));
            bw.append(content);
            bw.newLine();
            bw.close();
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    private HTable getTable() {
        return table;
    }

    private void setTable(HTable table) {
        this.table = table;
    }

    private Map<String, Long> getIntervalTopTopic() {
        return intervalTopTopic;
    }

    private void setIntervalTopTopic(Map<String, Long> intervalTopTopic) {
        this.intervalTopTopic = intervalTopTopic;
    }
}
