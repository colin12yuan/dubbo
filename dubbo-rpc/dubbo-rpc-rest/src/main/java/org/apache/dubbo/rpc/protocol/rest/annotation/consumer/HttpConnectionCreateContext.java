/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.protocol.rest.annotation.consumer;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.metadata.rest.RestMethodMetadata;
import org.apache.dubbo.metadata.rest.ServiceRestMetadata;
import org.apache.dubbo.remoting.http.RequestTemplate;
import org.apache.dubbo.rpc.Invocation;

public class HttpConnectionCreateContext {

    private RequestTemplate requestTemplate;
    private RestMethodMetadata restMethodMetadata;
    private ServiceRestMetadata serviceRestMetadata;
    private Invocation invocation;
    private URL url;

    public HttpConnectionCreateContext() {}

    public void setRequestTemplate(RequestTemplate requestTemplate) {
        this.requestTemplate = requestTemplate;
    }

    public RequestTemplate getRequestTemplate() {
        return requestTemplate;
    }

    public ServiceRestMetadata getServiceRestMetadata() {
        return serviceRestMetadata;
    }

    public RestMethodMetadata getRestMethodMetadata() {
        return restMethodMetadata;
    }

    public void setRestMethodMetadata(RestMethodMetadata restMethodMetadata) {
        this.restMethodMetadata = restMethodMetadata;
    }

    public Invocation getInvocation() {
        return invocation;
    }

    public void setInvocation(Invocation invocation) {
        this.invocation = invocation;
    }

    public URL getUrl() {
        return url;
    }

    public void setUrl(URL url) {
        this.url = url;
    }

    public void setServiceRestMetadata(ServiceRestMetadata serviceRestMetadata) {
        this.serviceRestMetadata = serviceRestMetadata;
    }
}
