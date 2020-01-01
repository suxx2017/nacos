/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.controllers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.core.utils.WebUtils;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.core.ServiceManager;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.web.CanDistro;

/**
 * Instance operation controller
 *
 * @author suxingrui
 */
@RestController
@RequestMapping(UtilsAndCommons.NACOS_NAMING_CONTEXT + "/instance")
public class InstanceControllerEx {
    
    @Autowired
    private SwitchDomain switchDomain;

    @Autowired
    private ServiceManager serviceManager;

	@CanDistro
	@RequestMapping(value = "/shutdown", method = RequestMethod.PUT)
	public String shutdown(HttpServletRequest request) throws Exception {
		String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
		String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);

		Instance instance = parseInstance(request);
		serviceManager.registerInstance(namespaceId, serviceName, instance);

		Service service = serviceManager.getService(namespaceId, serviceName);
		if (service == null) {
			throw new NacosException(NacosException.INVALID_PARAM,
					"service not found, namespace: " + namespaceId + ", service: " + serviceName);
		}
		List<Instance> allIPs = service.allIPs();
		int otherEnableCount = 0;
		for (Instance inst : allIPs) {
			if (!inst.getInstanceId().equals(instance.getInstanceId()) && inst.isEnabled() && inst.getWeight() > 0.0D) {
				otherEnableCount++;
			}
		}
		if (otherEnableCount < 1) {
			throw new NacosException(NacosException.SERVER_ERROR, "当前其他可用服务数少于1，不能停止。");
		}
		String url = "http://" + instance.getIp() + ":" + instance.getPort() + "/remote/instance/shutdown";
		Map<String, String> params = new HashMap<>(1);
		params.put("key", "55ac348750604eb593c493abf736686f");
		HttpClient.asyncHttpPost(url, null, params, null);
		return "ok";
	}

    private Instance parseInstance(HttpServletRequest request) throws Exception {

        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        String app = WebUtils.optional(request, "app", "DEFAULT");
        String metadata = WebUtils.optional(request, "metadata", StringUtils.EMPTY);

        Instance instance = getIPAddress(request);
        instance.setApp(app);
        instance.setServiceName(serviceName);
        instance.setInstanceId(instance.generateInstanceId());
        instance.setLastBeat(System.currentTimeMillis());
        if (StringUtils.isNotEmpty(metadata)) {
            instance.setMetadata(UtilsAndCommons.parseMetadata(metadata));
        }

        instance.validate();

        return instance;
    }

    private Instance getIPAddress(HttpServletRequest request) {

        String ip = WebUtils.required(request, "ip");
        String port = WebUtils.required(request, "port");
        String weight = WebUtils.optional(request, "weight", "1");
        String cluster = WebUtils.optional(request, CommonParams.CLUSTER_NAME, StringUtils.EMPTY);
        if (StringUtils.isBlank(cluster)) {
            cluster = WebUtils.optional(request, "cluster", UtilsAndCommons.DEFAULT_CLUSTER_NAME);
        }
        boolean healthy = BooleanUtils.toBoolean(WebUtils.optional(request, "healthy", "true"));

        String enabledString = WebUtils.optional(request, "enabled", StringUtils.EMPTY);
        boolean enabled;
        if (StringUtils.isBlank(enabledString)) {
            enabled = BooleanUtils.toBoolean(WebUtils.optional(request, "enable", "true"));
        } else {
            enabled = BooleanUtils.toBoolean(enabledString);
        }

        boolean ephemeral = BooleanUtils.toBoolean(WebUtils.optional(request, "ephemeral",
            String.valueOf(switchDomain.isDefaultInstanceEphemeral())));

        Instance instance = new Instance();
        instance.setPort(Integer.parseInt(port));
        instance.setIp(ip);
        instance.setWeight(Double.parseDouble(weight));
        instance.setClusterName(cluster);
        instance.setHealthy(healthy);
        instance.setEnabled(enabled);
        instance.setEphemeral(ephemeral);

        return instance;
    }

    @CanDistro
    @RequestMapping(value = "/available", method = RequestMethod.GET)
    public String available(HttpServletRequest request) throws Exception {
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID,
            Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        String cluster = WebUtils.optional(request, CommonParams.CLUSTER_NAME, UtilsAndCommons.DEFAULT_CLUSTER_NAME);
        String ip = WebUtils.required(request, "ip");
        int port = Integer.parseInt(WebUtils.required(request, "port"));

        Service service = serviceManager.getService(namespaceId, serviceName);
        if (service == null) {
            throw new NacosException(NacosException.NOT_FOUND, "no service " + serviceName + " found!");
        }

        List<String> clusters = new ArrayList<>();
        clusters.add(cluster);

        List<Instance> ips = service.allIPs(clusters);
        if (ips == null || ips.isEmpty()) {
            return "";
        }

        for (Instance instance : ips) {
            if (instance.getIp().equals(ip) && instance.getPort() == port) {
                return instance.isEnabled() && instance.getWeight() > 0.0D ? "true" : "false";
            }
        }

        return "";
    }
}
