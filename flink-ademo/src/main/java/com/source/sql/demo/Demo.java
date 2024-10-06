package com.source.sql.demo;

import com.source.sql.javacc.parser.select.ParseException;
import com.source.sql.javacc.parser.select.SimpleSelectParser;

public class Demo {
    public static void main(String[] args) throws ParseException {
        parseSelect("select 1+1");
        parseSelect("select 2-1");// select id ,name from table
    }
    private static void parseSelect(String sql) throws ParseException {
        final SimpleSelectParser parser = new SimpleSelectParser(sql);
//        // 解析的核心方法
        parser.parse();
    }

}
