package com.example.bigdata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CsvReader {
    public static List<List<String>> readCsv() throws IOException {
        List<List<String>> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("static/Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting__IUCR__Codes.csv"))) {
            String line;
            while ((line = br.readLine()) != null) {
                records.add(parseLine(line));
            }
        }
        return records;
    }

    private static List<String> parseLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (char c : line.toCharArray()) {
            if (inQuotes) {
                if (c == '\"') {
                    inQuotes = false;
                } else {
                    current.append(c);
                }
            } else {
                if (c == '\"') {
                    inQuotes = true;
                } else if (c == ',') {
                    result.add(current.toString());
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            }
        }
        result.add(current.toString()); // add the last field
        return result;
    }
}