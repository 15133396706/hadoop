package org.apache.hadoop.yarn.server.federation.policies.router;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.RouterPolicyFacade;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IntelligentAlgorithmPolicy extends AbstractRouterPolicy {

    private static final Logger LOG =
            LoggerFactory.getLogger(RouterPolicyFacade.class);

    //mapreduce.job.tags=taskid,cluster_id,wait_time,priority
    @Override
    public SubClusterId getHomeSubcluster(
            ApplicationSubmissionContext appSubmissionContext,
            List<SubClusterId> blackListSubClusters) throws YarnException {

        // null checks and default-queue behavior
        validate(appSubmissionContext);

        Map<SubClusterId, SubClusterInfo> activeSubclusters = getActiveSubclusters();

        FederationPolicyUtils.validateSubClusterAvailability(
                new ArrayList<SubClusterId>(activeSubclusters.keySet()), blackListSubClusters);

        // Get cluster id
        Set<String> tagsSet = appSubmissionContext.getApplicationTags();
        SubClusterIdInfo chosen = null;
        for (String tag : tagsSet) {
            if (!(null == tag || tag.isEmpty())) {
                //TODO: ###匹配
                if (tag.contains("#")) {
                    String[] tags = tag.split("#");
                    if (tag.length() == 4 && !tags[1].isEmpty()) {
                        chosen = new SubClusterIdInfo(tags[1]);
                        break;
                    }
                }
            }
        }

        // Check whether the selected cluster is active
        if (null != chosen && activeSubclusters.keySet().contains(chosen.toId())) {
            LOG.info("AlgorithmPolicy, subClusterId: " + chosen.toId().toString());
            return chosen.toId();
        }

        // Load-based rollback policy
        long currBestMem = -1;
        for (Map.Entry<SubClusterId, SubClusterInfo> entry : activeSubclusters
                .entrySet()) {

            SubClusterIdInfo id = new SubClusterIdInfo(entry.getKey());
            long availableMemory = getAvailableMemory(entry.getValue());
            if (availableMemory > currBestMem) {
                currBestMem = availableMemory;
                chosen = id;
            }
        }
        if (chosen == null) {
            throw new FederationPolicyException(
                    "Unable to select an active subCluster");
        }
        LOG.info("LoadBasedRouterPolicy, subClusterId: " + chosen.toId().toString());
        return chosen.toId();
    }

    private long getAvailableMemory(SubClusterInfo value) throws YarnException {
        try {
            long mem = -1;
            JSONObject obj = new JSONObject(value.getCapability());
            mem = obj.getJSONObject("clusterMetrics").getLong("availableMB");
            return mem;
        } catch (JSONException j) {
            throw new YarnException("FederationSubCluserInfo cannot be parsed", j);
        }
    }
}
