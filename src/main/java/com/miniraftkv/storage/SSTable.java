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
        }
