package org.apache.hadoop.yarn.server.federation.policies.manager;

import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.HomeAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.router.IntelligentAlgorithmPolicy;

/**
 * This class represents a simple implementation of a {@code
 * FederationPolicyManager}.
 *
 * It combines the basic policies: {@link IntelligentAlgorithmPolicy} and
 * {@link HomeAMRMProxyPolicy}, which are designed to work together.
 *
 */

public class CustomPolicyManager extends AbstractPolicyManager  {

    public CustomPolicyManager() {
        // this structurally hard-codes two compatible policies for Router and
        // AMRMProxy.
        routerFederationPolicy = IntelligentAlgorithmPolicy.class;
        amrmProxyFederationPolicy = HomeAMRMProxyPolicy.class;
    }
}
