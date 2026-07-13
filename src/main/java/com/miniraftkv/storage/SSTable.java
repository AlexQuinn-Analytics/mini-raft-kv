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
