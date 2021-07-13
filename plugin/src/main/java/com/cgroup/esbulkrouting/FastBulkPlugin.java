package com.cgroup.esbulkrouting;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * Created by zzq on 2021/7/5.
 */
public class FastBulkPlugin extends Plugin implements ActionPlugin {
    protected final org.apache.logging.log4j.Logger logger = LogManager.getLogger(this.getClass());

    /**
     * 在这里增加一个http接口
     *
     * @return
     */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(FastBulkAction.INSTANCE, FastBulkTransportAction.class));
    }

    public FastBulkPlugin() {
        super();
        logger.info("FastBulkPlugin初始化");
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController,
                                             ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings,
                                             SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(new FastBulkRestHandler(settings, restController,indexScopedSettings));
    }
}
