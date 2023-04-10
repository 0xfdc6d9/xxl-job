package com.xxl.job.core.biz;

import com.xxl.job.core.biz.model.*;

/**
 * Created by xuxueli on 17/3/1.
 */

/*
Q：biz 代表什么
A：在 XXL-JOB 中，biz 通常代表业务逻辑，也就是处理具体业务的代码逻辑。
在 core 包下的 biz 包中，包含了一些业务相关的类和接口，其中 ExecutorBiz 接口是执行器与调度器通信的关键接口之一。
ExecutorBiz 定义了执行器与调度器之间的通信协议，包括任务的注册、执行、停止、心跳等操作，执行器通过实现 ExecutorBiz 接口，与调度器进行通信，实现任务的执行和调度。
因此，biz 包下的 ExecutorBiz 接口是 XXL-JOB 中非常重要的一个组件，它是整个执行器与调度器通信的基础。
 */

public interface ExecutorBiz {

    /**
     * beat
     * @return
     */
    public ReturnT<String> beat();

    /**
     * idle beat
     *
     * @param idleBeatParam
     * @return
     */
    public ReturnT<String> idleBeat(IdleBeatParam idleBeatParam);

    /**
     * run
     * @param triggerParam
     * @return
     */
    public ReturnT<String> run(TriggerParam triggerParam);

    /**
     * kill
     * @param killParam
     * @return
     */
    public ReturnT<String> kill(KillParam killParam);

    /**
     * log
     * @param logParam
     * @return
     */
    public ReturnT<LogResult> log(LogParam logParam);

}
