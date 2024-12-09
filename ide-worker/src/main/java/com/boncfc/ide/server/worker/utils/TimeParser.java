package com.boncfc.ide.server.worker.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeParser {

    public static Date parseExpression(Date baseDate, String expression) throws ParseException {
        // 确定基准日期
        if (expression.startsWith("{") && expression.endsWith("}")) {
            Calendar cal = Calendar.getInstance();
            if (baseDate != null) {
                cal.setTime(baseDate);
            }
            else{
                // {} 表示昨天的日期,基准日期取当前时间的前一日
                cal = Calendar.getInstance();
            }
            cal.add(Calendar.DATE, -1);
            baseDate = cal.getTime();

        } else if (expression.startsWith("[") && expression.endsWith("]")) {
            if (baseDate == null) {
                // [] 表示今天的日期
                baseDate = new Date();
            }
        } else {
            throw new IllegalArgumentException("表达式格式不正确");
        }
        // 提取格式、操作符和数值 N
        String content = expression.substring(1, expression.length() - 1);
        String pattern = "(.*)\\s*([+-])\\s*(\\d+)";
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(content);
        if (!m.matches()) {
            throw new IllegalArgumentException("表达式内容格式不正确");
        }
        String dateFormat = m.group(1).trim();
        String operator = m.group(2);
        int N = Integer.parseInt(m.group(3));

        // 找到格式中的最小单位
        char smallestUnit = getSmallestUnit(dateFormat);
        int calendarField = getCalendarField(smallestUnit);

        // 格式化基准日期
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        String formattedDateStr = sdf.format(baseDate);
        Date formattedDate = sdf.parse(formattedDateStr);

        // 计算最终日期
        Calendar cal = Calendar.getInstance();
        cal.setTime(formattedDate);
        if (operator.equals("+")) {
            cal.add(calendarField, N);
        } else if (operator.equals("-")) {
            cal.add(calendarField, -N);
        } else {
            throw new IllegalArgumentException("操作符不正确");
        }

        return cal.getTime();
    }

    // 获取格式中的最小单位
    private static char getSmallestUnit(String dateFormat) {
        Set<Character> patternLetters = new HashSet<>(Arrays.asList(
                'G', 'y', 'M', 'w', 'W', 'D', 'd', 'F', 'E', 'u',
                'a', 'H', 'k', 'K', 'h', 'm', 's', 'S', 'z', 'Z', 'X'
        ));
        for (int i = dateFormat.length() - 1; i >= 0; i--) {
            char c = dateFormat.charAt(i);
            if (patternLetters.contains(c)) {
                return c;
            }
        }
        throw new IllegalArgumentException("格式中未找到有效的时间单位");
    }

    // 将格式字符映射到 Calendar 字段
    private static int getCalendarField(char patternChar) {
        switch (patternChar) {
            case 'S':
                return Calendar.MILLISECOND;
            case 's':
                return Calendar.SECOND;
            case 'm':
                return Calendar.MINUTE;
            case 'H':
            case 'k':
            case 'K':
            case 'h':
                return Calendar.HOUR_OF_DAY;
            case 'E':
                return Calendar.DAY_OF_WEEK;
            case 'd':
                return Calendar.DAY_OF_MONTH;
            case 'D':
                return Calendar.DAY_OF_YEAR;
            case 'F':
                return Calendar.DAY_OF_WEEK_IN_MONTH;
            case 'W':
                return Calendar.WEEK_OF_MONTH;
            case 'w':
                return Calendar.WEEK_OF_YEAR;
            case 'M':
                return Calendar.MONTH;
            case 'y':
                return Calendar.YEAR;
            case 'u':
                return Calendar.DAY_OF_WEEK; // ISO标准的星期数，Java中没有直接对应，可能需要额外处理
            default:
                throw new IllegalArgumentException("不支持的时间单位：" + patternChar);
        }
    }
    // 新增方法：根据表达式返回格式化后的日期字符串
    public static String formatExpressionResult(Date baseDate, String expression) throws ParseException {
        Date date = parseExpression(baseDate, expression);

        // 提取日期格式
        String content = expression.substring(1, expression.length() - 1).trim();

        // 处理特殊关键字，如 NEXT MONDAY、LAST TUESDAY
        String dateFormat;
        if (content.toUpperCase().startsWith("NEXT ") || content.toUpperCase().startsWith("LAST ")) {
            // 如果存在日期格式，则提取出来
            String[] parts = content.split("\\s+");
            if (parts.length > 2) {
                StringBuilder dateFormatBuilder = new StringBuilder();
                for (int i = 2; i < parts.length; i++) {
                    dateFormatBuilder.append(parts[i]).append(" ");
                }
                dateFormat = dateFormatBuilder.toString().trim();
            } else {
                // 默认格式
                dateFormat = "yyyy-MM-dd";
            }
        } else {
            // 原有的格式解析逻辑
            String pattern = "(.*)\\s*([+-])\\s*(\\d+)";
            Pattern p = Pattern.compile(pattern);
            Matcher m = p.matcher(content);
            if (!m.matches()) {
                throw new IllegalArgumentException("表达式内容格式不正确");
            }
            dateFormat = m.group(1).trim();
        }

        // 使用提取的格式格式化日期
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        return sdf.format(date);
    }
    public static void main(String[] args) throws ParseException {
        TimeParser parser = new TimeParser();
//        String expression = "{yyyyMMdd+1}";
        String expression2 = "{yyyyMMdd+0}";
        String resultDateStr = parser.formatExpressionResult(new Date(), expression2);
//        Date resultDate = parser.parseExpression(expression2);
        System.out.println("解析结果日期str：" + resultDateStr);
//        System.out.println("解析结果日期：" + resultDate);

//         格式化输出结果，便于查看
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd EEEE");
//        System.out.println("格式化后的日期：" + sdf.format(resultDate));
    }
}
