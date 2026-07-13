package com.miniraftkv.storage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Map;

public class SSTable {
    private String filename;

    public SSTable(String filename) {
        this.filename = filename;
    }
    
    public void flush(Map<String, String> data) {
        try {
            FileWriter writer = new FileWriter(filename);
            for (Map.Entry<String, String> entry : data.entrySet()) {
                writer.write(entry.getKey() + "=" + entry.getValue() + "\n");
            }
            writer.close()
            System.out.println("[SSTable] Flushed " + data.size() 
                + " entries to " + filename);
        } catch (Exception e) {
            System.out.println("[SSTable] Flush failed: " + e.getMessage());
        }
    }

     // Get: read a key from this SSTable file
    public String get(String key) {
        try {
            File file = new File(filename);
            if (!file.exists()) {
                return null;
            }
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=", 2);
                if (parts[0].equals(key)) {
                    reader.close();
                    return parts[1];   // found
                }
            }
            reader.close();
            return null;   // not found
