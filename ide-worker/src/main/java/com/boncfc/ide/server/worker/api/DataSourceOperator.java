package com.boncfc.ide.server.worker.api;


import com.boncfc.ide.plugin.task.api.datasource.ConnectionParam;
import com.boncfc.ide.plugin.task.api.datasource.DataSourceProcessorProvider;
import com.boncfc.ide.plugin.task.api.datasource.DbType;
import com.boncfc.ide.plugin.task.api.model.DatasourceDetailInfo;
import com.boncfc.ide.plugin.task.api.utils.DataSourceUtils;
import com.boncfc.ide.server.worker.common.model.Result;
import com.boncfc.ide.server.worker.common.model.Status;
import com.boncfc.ide.server.worker.config.WorkerConfig;
import com.boncfc.ide.server.worker.mapper.WorkerMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/ide/worker/datasources")
public class DataSourceOperator {

    @Autowired
    WorkerMapper workerMapper;

    @Autowired
    WorkerConfig workerConfig;

    @PostMapping(value = "/{dsId}/testconnect")
    @ResponseStatus(HttpStatus.OK)
    public Result testConnect(@PathVariable(value = "dsId") Integer dsId) {
        List<Integer> dsIds = new LinkedList<>();
        dsIds.add(dsId);
        List<DatasourceDetailInfo> datasourceDetailInfoList = workerMapper.getDatasourceDetailInfoList(dsIds);
        datasourceDetailInfoList.forEach(datasourceDetailInfo -> {
            datasourceDetailInfo.setDsPasswordAESKey(workerConfig.getDatasourcePasswordAesKey());
        });
        DatasourceDetailInfo detailInfo = datasourceDetailInfoList.get(0);
        DbType dbType = DbType.valueOf(detailInfo.getDsType());
        ConnectionParam connectionParam = DataSourceUtils.buildConnectionParams(dbType, detailInfo);
        try(Connection connection =
                    DataSourceProcessorProvider.getDataSourceProcessor(dbType).getConnection(connectionParam)) {
            return Result.success();
        } catch (Exception e) {
            return Result.error(Status.DATASOURCE_CONNECT_FAILED);
        }

    }
}
