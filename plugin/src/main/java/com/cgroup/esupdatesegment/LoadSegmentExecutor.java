package com.cgroup.esupdatesegment;

import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by zzq on 2021/6/11.
 */
public class LoadSegmentExecutor {
    public long commitSegment(String indexName, String indexUUID, int shardIdNo, String documentPrimeKey,
                              List<String> segmentDirs, IndicesService indicesService)
            throws RuntimeException, IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        long removeCount = 0;

        paramsCheck(indexName, indexUUID, shardIdNo, documentPrimeKey, segmentDirs, indicesService);
        /**
         * 获得shard信息
         */
        ShardId shardId = new ShardId(indexName, indexUUID, shardIdNo);
        IndexShard shard = indicesService.getShardOrNull(shardId);
        if (shard == null) {
            throw new RuntimeException("shard not found, indexName:" + indexName + ", shardId:" + shardIdNo);
        }
        /**
         * =====两次反射，获取shard操作对象=====
         */
        //step1 获得lucene的IndexWriter对象，需要反射调用，原方法访问受限，也可通过
        /* FIXME 可以通过修改es的代码, 将lucene的IndexWriter对象暴露给plugin使用  */
        Class<? extends IndexShard> shardClass = shard.getClass();
        Method getEngineOrNullMethod = shardClass.getDeclaredMethod("getEngineOrNull");
        getEngineOrNullMethod.setAccessible(true);
        Object internalEngineObj = getEngineOrNullMethod.invoke(shard);
        if (internalEngineObj == null) {
            throw new RuntimeException("调用getEngineOrNull方法返回值为null，可能由于Lucene未启动");
        }
        InternalEngine internalEngine = (InternalEngine) internalEngineObj;
        //step2 反射私有方法getIndexWriter获取操作ES的shard对象
        Class<? extends InternalEngine> internalEngineClass = internalEngine.getClass();
        Field lIndexWriterField = internalEngineClass.getDeclaredField("indexWriter");
        lIndexWriterField.setAccessible(true);
        Object lIndexWriterObj = lIndexWriterField.get(internalEngine);
        if (lIndexWriterObj == null) {
            throw new RuntimeException("调用getEngineOrNull方法返回值为null，可能由于Lucene未启动");
        }
        IndexWriter lIndexWriter = (IndexWriter) lIndexWriterObj;
        //step3 获取内部引擎中的版本管理映射表
        Field versionMapField = internalEngineClass.getDeclaredField("versionMap");
        versionMapField.setAccessible(true);
        Object versionMapObj = versionMapField.get(internalEngine);
        if (versionMapObj == null) {
            throw new RuntimeException("获取versionMap属性值为null，反射失败");
        }
        Class<?> versionMapClass = versionMapObj.getClass();
        Method removeTombstoneUnderLockMethod = versionMapClass.getDeclaredMethod("removeTombstoneUnderLock", BytesRef.class);
        removeTombstoneUnderLockMethod.setAccessible(true);
        /**
         * 以新segment为准，移除掉旧的segment中数据；并且将新的lucene（segment）文件加入到shard中
         */
        Directory[] segmentDirectories = new Directory[segmentDirs.size()];
        for (int i = 0; i < segmentDirs.size(); i++) {
            FSDirectory directory = MMapDirectory.open(Paths.get(segmentDirs.get(i)));
            //如果原Segment中没有文档，则直接跳过垃圾文档清除阶段
            if (lIndexWriter.getDocStats().numDocs > 0) {
                removeCount += removeDuplicateDocument(directory, lIndexWriter, documentPrimeKey,
                        versionMapObj, removeTombstoneUnderLockMethod);
            }
            segmentDirectories[i] = directory;
        }
        lIndexWriter.addIndexes(segmentDirectories);
        lIndexWriter.commit();

        return removeCount;
    }

    /**
     * 变量本地新加的lucene文件(segment)和原shard中segment数据主键_uid做对比
     * 如果存在冲突，则删除shard中原segment的数据
     *
     * @param directory        本地新的luceneSegment文件目录句柄
     * @param dstLIndexWriter  原shard写入句柄对象，对象是LuceneIndex
     * @param documentPrimeKey 固定值：_uid是es内部的文档id属性key
     * @return
     * @throws IOException
     */
    public long removeDuplicateDocument(FSDirectory directory, IndexWriter dstLIndexWriter, String documentPrimeKey,
                                        Object versionMapObj, Method removeTombstoneUnderLockMethod) throws IOException {
        long count = 0L;

        IndexWriter srcIndexWriter = null;
        StandardDirectoryReader srcReader = null;
        StandardDirectoryReader dstReader = null;
        try {
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(null);
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
            indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);

            srcIndexWriter = new IndexWriter(directory, indexWriterConfig);

            srcReader = (StandardDirectoryReader) DirectoryReader.open(srcIndexWriter);
            dstReader = (StandardDirectoryReader) DirectoryReader.open(dstLIndexWriter);

            // 遍历src中各个segment
            for (LeafReaderContext srcLeafReaderContext : srcReader.leaves()) {
                LeafReader srcLeafReader = srcLeafReaderContext.reader();

                Terms srcTerms = srcLeafReader.terms(documentPrimeKey);
                if (srcTerms == null) {
                    continue;
                }

                TermsEnum srcTermsEnum = srcTerms.iterator();

                // 遍历单个segment中各个主键
                for (; srcTermsEnum.next() != null; ) {
                    BytesRef bytesRef = srcTermsEnum.term();
                    Term srcTerm = new Term(documentPrimeKey, bytesRef);
                    // 判断dst中是否存在相同的主键
                    for (LeafReaderContext dstLeafReaderContext : dstReader.leaves()) {
                        LeafReader dstLeafReader = dstLeafReaderContext.reader();
                        if (dstLeafReader.postings(srcTerm) != null) {
                            // 如果碰到相同主键，则删除dst中对应的主键
                            dstLIndexWriter.deleteDocuments(srcTerm);
                            count++;
                            //刷入的数据，需要在InternalEngine中的垃圾版本控制数据立即清除
                            removeTombstoneUnderLockMethod.invoke(versionMapObj, bytesRef);
                            break;
                        }
                        //刷入的数据，需要在InternalEngine中的垃圾版本控制数据立即清除
                        removeTombstoneUnderLockMethod.invoke(versionMapObj, bytesRef);
                    }
                }
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } finally {
            if (srcReader != null) {
                srcReader.close();
            }

            if (dstReader != null) {
                dstReader.close();
            }

            if (srcIndexWriter != null) {
                srcIndexWriter.close();
            }
        }

        return count;
    }

    public void paramsCheck(String indexName, String indexUUID, int shardIdNo, String documentPrimeKey,
                            List<String> segmentDirs, IndicesService indicesService) {
        if (segmentDirs == null || segmentDirs.size() < 1) {
            throw new RuntimeException("segmentDirs不可以是null或empty");
        }
        if (documentPrimeKey == null || documentPrimeKey.length() < 1) {
            throw new RuntimeException("primeKey不可以是null或empty");
        }
        if (indexName == null || indexName.length() < 1) {
            throw new RuntimeException("indexName不可以是null或empty");
        }
        if (indexUUID == null || indexUUID.length() < 1) {
            throw new RuntimeException("es中IndexUUID不可以是null或empty");
        }
        if (shardIdNo < 0) {
            throw new RuntimeException("shardIdNo是不会小于0的");
        }
        if (indicesService == null) {
            throw new RuntimeException("IndicesService对象不允许为空，检查es引擎是否启动");
        }
    }
}
