package com.boncfc.ide.plugin.task.api.model;

public enum JobInstanceExtStatus {
    wait_predecessors,wait_schedule,wait_jf_para,wait_res_limit,
    inp_to_get,inp_torun,inp_running,inp_killing,
    success_force,success_dry,success_normal,success_ignore,
    fail_normal,fail_killed
}
