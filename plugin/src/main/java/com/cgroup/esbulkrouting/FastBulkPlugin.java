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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.function.Supplier;

import static com.cgroup.esbulkrouting.FastBulkAction.SETTING_ROUTING_SLOT;

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

    public FastBulkPlugin() throws NoSuchFieldException, IllegalAccessException {
        super();
        logger.info("FastBulkPlugin初始化");
        addRoutingSlotProperty();
    }

    /**
     * 最好的时机，向索引setting列表添加routing_slot参数
     *
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    private void addRoutingSlotProperty() throws NoSuchFieldException, IllegalAccessException {
        Field built_in_index_settingsField = IndexScopedSettings.class.getDeclaredField("BUILT_IN_INDEX_SETTINGS");
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(built_in_index_settingsField, built_in_index_settingsField.getModifiers() & ~Modifier.FINAL);

        Setting<Integer> integerSetting = Setting.intSetting(SETTING_ROUTING_SLOT, 1, 1, 1024,
                Setting.Property.Dynamic, Setting.Property.IndexScope);

        Set<Setting<?>> settingSet = new HashSet<>();
        settingSet.addAll(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        settingSet.add(integerSetting);

        built_in_index_settingsField.set(null, settingSet);
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController,
                                             ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings,
                                             SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(new FastBulkRestHandler(settings, restController));
    }
}
