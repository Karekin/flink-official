package com.source.sql.demo;

public class Demo {
    public static void main(String[] args) {
        parseSelect("select 1+1");
        parseSelect("select 2-1");// select id ,name from table
    }
    private static void parseSelect(String sql) {
        //final SimpleSelectParser parser = new SimpleSelectParser(sql);
        // 解析的核心方法
        //parser.parse();
    }

}
