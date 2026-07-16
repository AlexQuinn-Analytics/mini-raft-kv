package com.miniraftkv.storage;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class LSMStore {
    private MemTable memTable;
    private List<SSTable> ssTables;
    private int maxMemTableSize;
    private int ssTableCounter = 0;

    public LSMStore(int maxMemTableSize) {
        this.maxMemTableSize = maxMemTableSize;
        this.memTable = new MemTable(maxMemTableSize);
        this.ssTables = new ArrayList<>();
    }
    
    public void put(String key, String value) {
        memTable.put(key, value);
        if (memTable.isFull()) {
            flush();
        }
    }

    private void flush() {
        ssTableCounter++;
        String filename = "sstable_" + ssTableCounter + ".txt";
        SSTable ssTable = new SSTable(filename);
        ssTable.flush(memTable.getAll());
        ssTables.add(ssTable);
        memTable.clear();
    }

    public String get(String key) {
        String value = memTable.get(key);
        if (value != null) {
            return value;
        }
        for (int i = ssTables.size() - 1; i >= 0; i--) {
            value = ssTables.get(i).get(key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }
