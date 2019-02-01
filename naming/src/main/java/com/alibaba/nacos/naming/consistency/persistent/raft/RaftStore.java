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
package com.alibaba.nacos.naming.consistency.persistent.raft;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.nacos.naming.consistency.ApplyAction;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.nacos.common.util.SystemUtils.NACOS_HOME;
import static com.alibaba.nacos.common.util.SystemUtils.NACOS_HOME_KEY;

/**
 * @author nacos
 */
@Component
public class RaftStore {

    private static String BASE_DIR = NACOS_HOME + File.separator + "raft";

    private static String META_FILE_NAME;

    private Properties meta = new Properties();

    private static String CACHE_DIR;

    static {

        if (StringUtils.isNotBlank(System.getProperty(NACOS_HOME_KEY))) {
            BASE_DIR = NACOS_HOME + File.separator + "data" + File.separator + "naming";
        }

        META_FILE_NAME = BASE_DIR + File.separator + "meta.properties";
        CACHE_DIR = BASE_DIR + File.separator + "data";
    }

    public synchronized ConcurrentHashMap<String, Datum<?>> loadDatums(RaftCore.Notifier notifier) throws Exception {

        ConcurrentHashMap<String, Datum<?>> datums = new ConcurrentHashMap<>(32);
        Datum datum;
        long start = System.currentTimeMillis();
        for (File cache : listCaches()) {
            if (cache.isDirectory() && cache.listFiles() != null) {
                for (File datumFile : cache.listFiles()) {
                    datum = readDatum(datumFile, cache.getName());
                    if (datum != null) {
                        datums.put(datum.key, datum);
                        notifier.addTask(datum, ApplyAction.CHANGE);
                    }
                }
                continue;
            }
            datum = readDatum(cache, StringUtils.EMPTY);
            if (datum != null) {
                datums.put(datum.key, datum);
            }
        }

        Loggers.RAFT.info("finish loading all datums, size: {} cost {} ms.", datums.size(), (System.currentTimeMillis() - start));
        return datums;
    }

    public synchronized Properties loadMeta() throws Exception {
        File metaFile = new File(META_FILE_NAME);
        if (!metaFile.exists() && !metaFile.getParentFile().mkdirs() && !metaFile.createNewFile()) {
            throw new IllegalStateException("failed to create meta file: " + metaFile.getAbsolutePath());
        }

        try (FileInputStream inStream = new FileInputStream(metaFile)) {
            meta.load(inStream);
        }
        return meta;
    }

    public synchronized Datum load(String key) throws Exception {
        long start = System.currentTimeMillis();
        // load data
        for (File cache : listCaches()) {
            if (!cache.isFile()) {
                Loggers.RAFT.warn("warning: encountered directory in cache dir: {}", cache.getAbsolutePath());
            }

            if (!StringUtils.equals(cache.getName(), encodeFileName(key))) {
                continue;
            }

            Loggers.RAFT.info("finish loading datum, key: {} cost {} ms.",
                key, (System.currentTimeMillis() - start));
            return readDatum(cache, StringUtils.EMPTY);
        }

        return null;
    }

    public synchronized static Datum readDatum(File file, String namespaceId) throws IOException {

        ByteBuffer buffer;
        FileChannel fc = null;
        try {
            fc = new FileInputStream(file).getChannel();
            buffer = ByteBuffer.allocate((int) file.length());
            fc.read(buffer);

            String json = new String(buffer.array(), "UTF-8");
            if (StringUtils.isBlank(json)) {
                return null;
            }

            if (KeyBuilder.matchServiceMetaKey(file.getName())) {
                try {
                    return JSON.parseObject(json.replace("\\", ""), new TypeReference<Datum<Service>>() {
                    });
                } catch (Exception e) {
                    Datum<String> datum = JSON.parseObject(json, new TypeReference<Datum<String>>() {
                    });
                    Datum<Service> serviceDatum = new Datum<>();
                    serviceDatum.timestamp.set(datum.timestamp.get());
                    serviceDatum.key = datum.key;
                    serviceDatum.value = JSON.parseObject(datum.value, Service.class);
                    return serviceDatum;
                }
            }

            if (KeyBuilder.matchInstanceListKey(file.getName())) {

                Datum<List<Instance>> datum = JSON.parseObject(json, new TypeReference<Datum<List<Instance>>>() {
                });
                Datum<Instances> instancesDatum = new Datum<>();
                instancesDatum.key = datum.key;
                instancesDatum.timestamp.set(datum.timestamp.get());

                Instances instances = new Instances();
                instances.setInstanceMap(new HashMap<>(16));
                for (Instance instance : datum.value) {
                    instances.getInstanceMap().put(instance.getDatumKey(), instance);
                }
                instancesDatum.value = instances;
                return instancesDatum;

            }

            return JSON.parseObject(json, Datum.class);

        } catch (Exception e) {
            Loggers.RAFT.warn("waning: failed to deserialize key: {}", file.getName());
            throw e;
        } finally {
            if (fc != null) {
                fc.close();
            }
        }
    }

    public synchronized static void write(final Datum datum) throws Exception {

        String namespaceId = KeyBuilder.getNamespace(datum.key);

        File cacheFile;

        if (StringUtils.isNotBlank(namespaceId)) {
            cacheFile = new File(CACHE_DIR + File.separator + namespaceId + File.separator + encodeFileName(datum.key));
        } else {
            cacheFile = new File(CACHE_DIR + File.separator + encodeFileName(datum.key));
        }

        if (!cacheFile.exists() && !cacheFile.getParentFile().mkdirs() && !cacheFile.createNewFile()) {
            MetricsMonitor.getDiskException().increment();

            throw new IllegalStateException("can not make cache file: " + cacheFile.getName());
        }

        FileChannel fc = null;
        ByteBuffer data;

        if (KeyBuilder.matchInstanceListKey(datum.key)) {
            Datum<Collection<Instance>> listDatum = new Datum<>();
            listDatum.key = datum.key;
            listDatum.value = ((Instances) datum.value).getInstanceMap().values();
            listDatum.timestamp.set(datum.timestamp.get());
            data = ByteBuffer.wrap(JSON.toJSONString(listDatum).getBytes("UTF-8"));
        } else {
            data = ByteBuffer.wrap(JSON.toJSONString(datum).getBytes("UTF-8"));
        }

        try {
            fc = new FileOutputStream(cacheFile, false).getChannel();
            fc.write(data, data.position());
            fc.force(true);
        } catch (Exception e) {
            MetricsMonitor.getDiskException().increment();
            throw e;
        } finally {
            if (fc != null) {
                fc.close();
            }
        }
    }

    private static File[] listCaches() throws Exception {
        File cacheDir = new File(CACHE_DIR);
        if (!cacheDir.exists() && !cacheDir.mkdirs()) {
            throw new IllegalStateException("cloud not make out directory: " + cacheDir.getName());
        }

        return cacheDir.listFiles();
    }

    public static void delete(Datum datum) {

        // datum key contains namespace info:
        String namespaceId = KeyBuilder.getNamespace(datum.key);

        if (StringUtils.isNotBlank(namespaceId)) {

            File cacheFile = new File(CACHE_DIR + File.separator + namespaceId + File.separator + encodeFileName(datum.key));
            if (cacheFile.exists() && !cacheFile.delete()) {
                Loggers.RAFT.error("[RAFT-DELETE] failed to delete datum: {}, value: {}", datum.key, datum.value);
                throw new IllegalStateException("failed to delete datum: " + datum.key);
            }
        }
    }

    public void updateTerm(long term) throws Exception {
        File file = new File(META_FILE_NAME);
        if (!file.exists() && !file.getParentFile().mkdirs() && !file.createNewFile()) {
            throw new IllegalStateException("failed to create meta file");
        }

        try (FileOutputStream outStream = new FileOutputStream(file)) {
            // write meta
            meta.setProperty("term", String.valueOf(term));
            meta.store(outStream, null);
        }
    }

    private static String encodeFileName(String fileName) {
        return fileName.replace(':', '#');
    }

    private static String decodeFileName(String fileName) {
        return fileName.replace("#", ":");
    }
}