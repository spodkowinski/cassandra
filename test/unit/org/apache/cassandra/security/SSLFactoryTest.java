/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.security;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.junit.Test;

public class SSLFactoryTest
{

    @Test
    public void testFilterCipherSuites()
    {
        String[] supported = new String[] {"x", "b", "c", "f"};
        String[] desired = new String[] { "k", "a", "b", "c" };
        assertArrayEquals(new String[] { "b", "c" }, SSLFactory.filterCipherSuites(supported, desired));

        desired = new String[] { "c", "b", "x" };
        assertArrayEquals(desired, SSLFactory.filterCipherSuites(supported, desired));
    }

    @Test
    public void testServerSocketCiphers() throws UnknownHostException, IOException
    {
        ServerEncryptionOptions options = new EncryptionOptions.ServerEncryptionOptions();
        options.keystore = "test/conf/keystore.jks";
        options.keystore_password = "cassandra";
        options.truststore = options.keystore;
        options.truststore_password = options.keystore_password;

        // expect JVM default ciphers for connection if no custom cipher list was specified 
        String[] defaultCipherSuites = ((SSLSocketFactory)SSLSocketFactory.getDefault()).getDefaultCipherSuites();
        assertArrayEquals(defaultCipherSuites, options.cipher_suites);

        SSLServerSocket socket = SSLFactory.getServerSocket(options, InetAddress.getLocalHost(), 55123);
        assertArrayEquals(defaultCipherSuites, socket.getEnabledCipherSuites());
        socket.close();

        // use custom ciphers for socket
        String[] custom_ciphers = new String[] {
                "TLS_RSA_WITH_AES_128_CBC_SHA",
                "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
                "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"
        };
        options.cipher_suites = custom_ciphers;

        socket = SSLFactory.getServerSocket(options, InetAddress.getLocalHost(), 55123);
        assertArrayEquals(custom_ciphers, socket.getEnabledCipherSuites());
        socket.close();
    }

}
