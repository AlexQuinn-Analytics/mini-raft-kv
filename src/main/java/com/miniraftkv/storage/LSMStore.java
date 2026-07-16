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
