package com.cgroup.essearch;

import com.cgroup.esbulkrouting.FastBulkAction;
import com.cgroup.esbulkrouting.FastBulkRestHandler;
import com.cgroup.esbulkrouting.FastBulkTransportAction;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * Created by zzq on 2021/7/20.
 */
public class SlotSearchPlugin extends Plugin implements ActionPlugin {
    protected final org.apache.logging.log4j.Logger logger = LogManager.getLogger(this.getClass());
    /**
     * 在这里增加一个http接口
     *
     * @return
     */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(SlotSearchAction.INSTANCE, SlotSearchTransportAction.class));
    }

    public SlotSearchPlugin() {
        super();
        logger.info("SlotSearchPlugin初始化（逻辑槽位映射物理shard）");
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController,
                                             ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings,
                                             SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(new SlotSearchRestHandler(settings, restController));
    }
}
