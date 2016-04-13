package com.kugou.whaledb;

import com.kugou.whaledb.hdfsDirectory.FileSystemDirectory;
import com.kugou.whaledb.utils.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Administrator on 2016/4/7 0007.
 */
public class DirectoryTest {
    @Test
    public void testFileSystemDirectory() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = new NoPermissionFileSystem();
        fs.initialize(FileSystem.getDefaultUri(conf), conf);
        Directory dir = new FileSystemDirectory(new Path("data"), new SingleInstanceLockFactory(), conf, fs);
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField("name", "yaotc", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher is = new IndexSearcher(reader);
        TopDocs docs = is.search(new TermQuery(new Term("name", "yaotc")), 1000);

        Assert.assertEquals("yaotc",is.doc(docs.scoreDocs[0].doc).get("name"));
    }

    @Test
    public void testWriteHdfsDirectory() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = HadoopUtil.getFs(conf);
        Directory dir = new FileSystemDirectory(new Path("data"), new SingleInstanceLockFactory(), conf, fs);
        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField("name", "yaotc", Field.Store.YES));
        writer.addDocument(doc);
        writer.commit();
        writer.close();
    }

    @Test
    public void testReadHdfsDirectory() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = HadoopUtil.getFs(conf);
        Directory dir = new FileSystemDirectory(new Path("data"), new SingleInstanceLockFactory(), conf, fs);
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher is = new IndexSearcher(reader);
        TopDocs docs = is.search(new TermQuery(new Term("name", "yaotc")), 1000);
        System.out.println(docs);
    }

    class NoPermissionFileSystem extends LocalFileSystem {
        public FSDataOutputStream create(Path f,
                                         boolean overwrite,
                                         int bufferSize
        ) throws IOException {
            return create(f, null, overwrite, bufferSize,
                    getDefaultReplication(f), getDefaultBlockSize(f), null);
        }
    }
}
