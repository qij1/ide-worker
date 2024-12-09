package com.boncfc.ide.server.worker.log;

import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * 〈日志纳秒记录〉
 *
 * @author 10180100
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class NanoTimeConvert extends ClassicConverter {
    @Override
    public String convert(ILoggingEvent iLoggingEvent) {
        return String.valueOf(System.nanoTime());
    }
}
