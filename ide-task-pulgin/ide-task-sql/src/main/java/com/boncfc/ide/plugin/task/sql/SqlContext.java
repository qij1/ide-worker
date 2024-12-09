package com.boncfc.ide.plugin.task.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SqlContext {
    String dsId;
    String querySql;
}
