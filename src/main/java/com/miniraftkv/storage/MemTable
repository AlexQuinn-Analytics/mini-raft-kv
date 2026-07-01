package com.miniraftkv.storage;

import java.util.TreeMap;
import java.util.Map;

public class MemTable {
    private TreeMap<String, String> table;
    private int maxSize;

    public MemTable(int maxSize) {
        this.table = new TreeMap<>();
        this.maxSize = maxSize;
    }

    public void put(String key, String value) {
        table.put(key, value);
    }

    public String get(String key) {
        return table.get(key);
    }

    public boolean isFull() {
        return table.size() >= maxSize;
    }

    public Map<String, String> getAll() {
        return table;
    }

    public int size() {
        return table.size();
    }

    public void clear() {
        table.clear();
    }
}

public static void main(String[] args) {
    MemTable mem = new MemTable(3);   // max 3 entries

    // Test 1: put and get
    mem.put("x", "5");
    mem.put("x", "8");   // overwrite
    mem.put("y", "10");

    System.out.println("x = " + mem.get("x"));      // expect 8 (overwritten)
    System.out.println("size = " + mem.size());     // expect 2 (x, y)
    System.out.println("full? " + mem.isFull());    // expect false (2 < 3)

    // Test 2: fill up
    mem.put("z", "20");
    System.out.println("full? " + mem.isFull());    // expect true (3 >= 3)

    // Test 3: ordered (TreeMap sorts by key)
    System.out.println("all = " + mem.getAll());     // expect {x=8, y=10, z=20} sorted

    // Test 4: missing key
    System.out.println("missing = " + mem.get("nokey"));  // expect null
}
