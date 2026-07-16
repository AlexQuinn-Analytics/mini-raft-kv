package com.miniraftkv.storage;

public class LSMTest {
    public static void main(String[] args) {
        LSMStore store = new LSMStore(3);

        // Write enough to create multiple SSTables
        store.put("a", "1");
        store.put("b", "2");
        store.put("c", "3");   // flush → sstable_1 (a=1, b=2, c=3)

        store.put("a", "99");  // a updated
        store.put("d", "4");
        store.put("e", "5");   // flush → sstable_2 (a=99, d=4, e=5)

        System.out.println("Before compact: " + store.getSSTableCount());  // expect 2
        System.out.println("a = " + store.get("a"));   // expect 99 (newest)

        // Compact!
        store.compact();

        System.out.println("After compact: " + store.getSSTableCount());   // expect 1
        System.out.println("a = " + store.get("a"));   // expect 99 (still newest!)
        System.out.println("b = " + store.get("b"));   // expect 2
        System.out.println("c = " + store.get("c"));   // expect 3
        System.out.println("d = " + store.get("d"));   // expect 4
        System.out.println("e = " + store.get("e"));   // expect 5
    }
}
