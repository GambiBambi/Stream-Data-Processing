package com.example.bigdata;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AccessLogRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = Logger.getLogger("Chicago");
    private static final String CRIME_PATTERN =
            // 1:ID 2:Date 3:IUCR 4:Arrest 5:Domestic 6:District, 7:ComArea 8:Latitude 9:Longitude
            "^(\\d+),([0-9T:\\.\\-]+Z),(\\S+),(True|False),(True|False),(\\d+\\.\\d+),(\"\"|\\d+\\.\\d+),(-?\\d+\\.\\d+),(-?\\d+\\.\\d+)$";

    private static final Pattern PATTERN = Pattern.compile(CRIME_PATTERN);
    private int ID;
    private String Date;
    private String IUCR;
    private Boolean Arrest;
    private Boolean Domestic;
    private Float District;
    private Float ComArea;
    private Float Latitude;
    private Float Longitude;
    private String Category;
    private String IndexCode;

    public static String findCategory(String IURC){
        for (List<String> row : ApacheLogToAlertRequests.csv_data) { // Loop through each row in csv_data
            String csv = "0";
            if (row.get(0).length() == 3) {
                csv += row.get(0);
            }
            else {
                csv = row.get(0);
            }
            if (csv.equals(IURC)) {
                return row.get(1);
            }
        }
        return "No matching category";
    }

    public static String findIndexCode(String IURC){
        for (List<String> row : ApacheLogToAlertRequests.csv_data) { // Loop through each row in csv_data
            String csv = "0";
            if (row.get(0).length() == 3) {
                csv += row.get(0);
            }
            else {
                csv = row.get(0);
            }
            if (csv.equals(IURC)) {
                if (row.get(row.size()-1).equals(".")) {
                    return null;
                }
                  return row.get(row.size()-1);
            }
        }
        return null;
    }

    public AccessLogRecord(String ID, String Date, String IUCR, String Arrest,
                            String Domestic, String District, String ComArea,
                            String Latitude, String Longitude) {
        //System.out.println("Access Log Record");
        this.setID(Integer.parseInt(ID));
        this.setDate(Date);
        this.setIUCR(IUCR);
        this.setArrest(Boolean.parseBoolean(Arrest));
        this.setDomestic(Boolean.parseBoolean(Domestic));
        this.setDistrict(Float.parseFloat(District));
        if (ComArea.equals("\"\"")) {
            ComArea = "0.0";
        }
        if (Latitude.equals("\"\"")) {
            Latitude = "0.0";
        }
        if (Longitude.equals("\"\"")) {
            Longitude = "0.0";
        }
        this.setComArea(Float.parseFloat(ComArea));
        this.setLatitude(Float.parseFloat(Latitude));
        this.setLongitude(Float.parseFloat(Longitude));
        this.Category = findCategory(IUCR);
        this.IndexCode = findIndexCode(IUCR);
    }

    public static AccessLogRecord parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            logger.log(Level.ALL, "Cannot parse logline" + logline);
            throw new RuntimeException("Error parsing logline: " + logline);
        }

        return new AccessLogRecord(m.group(1), m.group(2), m.group(3), m.group(4),
                m.group(5), m.group(6), m.group(7), m.group(8), m.group(9));
    }

    public static boolean lineIsCorrect(String logline) {
        Matcher m = PATTERN.matcher(logline);
        return m.find();
    }

    public long getTimestampInMillis() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX", Locale.US);
        Date date;
        try {
            date = sdf.parse(getDate());
            return date.getTime();
        } catch (ParseException e) {
            return -1;
        }
    }
    public long getTimestampInMonths() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX", Locale.US);
        Date date;
        try {
            date = sdf.parse(getDate());
            return date.getMonth();
        } catch (ParseException e) {
            return -1;
        }
    }

    public String getMonthYear() {
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM");
        String timestamp = getDate();
        try {
            Date date = inputFormat.parse(timestamp);
            return outputFormat.format(date);
        } catch (ParseException e) {
            return "0000-00";
        }
    }

    public int getID() {
        return ID;
    }

    public void setID(int ID) {
        this.ID = ID;
    }

    public String getDate() {
        return Date;
    }

    public void setDate(String date) {
        Date = date;
    }

    public String getIUCR() {
        return IUCR;
    }

    public void setIUCR(String IUCR) {
        this.IUCR = IUCR;
    }

    public Boolean getArrest() {
        return Arrest;
    }

    public void setArrest(Boolean arrest) {
        Arrest = arrest;
    }

    public Boolean getDomestic() {
        return Domestic;
    }

    public void setDomestic(Boolean domestic) {
        Domestic = domestic;
    }

    public Float getDistrict() {
        return District;
    }

    public void setDistrict(Float district) {
        District = district;
    }

    public Float getComArea() {
        return ComArea;
    }

    public void setComArea(Float comArea) {
        ComArea = comArea;
    }

    public Float getLatitude() {
        return Latitude;
    }

    public void setLatitude(Float latitude) {
        Latitude = latitude;
    }

    public Float getLongitude() {
        return Longitude;
    }

    public void setLongitude(Float longitude) {
        Longitude = longitude;
    }

    public String getCategory() {
        return Category;
    }

    public String getIndexCode() {
        return IndexCode;
    }

    public void setIndexCode(String indexCode) {
        IndexCode = indexCode;
    }

    public void setCategory(String category) {
        Category = category;
    }

    @Override
    public String toString() {
        return "Aggregate{" +
                "ID=" + ID +
                ", Date='" + Date + '\'' +
                ", IUCR=" + IUCR +
                ", Arrest=" + Arrest +
                ", Domestic=" + Domestic +
                ", District=" + District +
                ", ComArea=" + ComArea +
                ", Latitude=" + Latitude +
                ", Longitude=" + Longitude +
                '}';
    }
}
