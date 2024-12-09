package com.boncfc.ide.server.worker.log;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

/**
 * 〈worker log 过滤器〉
 *
 * @author 10180100
 * @see [相关类//方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class WorkerLogFilter extends Filter<ILoggingEvent> {

    @Override
    public FilterReply decide(ILoggingEvent event) {
        if (event.getThreadName().startsWith("Worker-")) {
            return FilterReply.ACCEPT;
        }
        return FilterReply.DENY;
    }
}