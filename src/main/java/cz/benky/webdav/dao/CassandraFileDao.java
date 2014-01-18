package cz.benky.webdav.dao;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.recipes.storage.CassandraChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.recipes.storage.ChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ObjectMetadata;
import cz.benky.webdav.util.CassandraUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

public class CassandraFileDao {
    private final ChunkedStorageProvider provider;
    private final Keyspace keyspace;

    public CassandraFileDao() {
        this.keyspace = CassandraUtils.getConnection();
        this.provider = new CassandraChunkedStorageProvider(keyspace, "storage");
    }

    public void deleteFile(UUID fileId) throws ConnectionException {
        ChunkedStorage.newDeleter(provider, fileId.toString());

        keyspace
                .prepareQuery(CassandraUtils.CQL3_CF)
                .withCql("DELETE FROM file WHERE file=?")
                .asPreparedStatement()
                .withUUIDValue(fileId)
                .execute();

    }

    public void writeFile(UUID objectName, InputStream is) {
        try {
            final ObjectMetadata call = ChunkedStorage.newWriter(provider, objectName.toString(), is).call();
            System.out.println(call);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void readFile(String objectName, OutputStream os) {
        try {
            ChunkedStorage.newReader(provider, objectName, os).call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public UUID createFile(final UUID root, final String name) throws ConnectionException {
        final UUID newResourceId = UUID.randomUUID();

        keyspace
                .prepareQuery(CassandraUtils.CQL3_CF)
                .withCql("INSERT INTO directory (pathId, child, childId) VALUES (?, ?, ?)")
                .asPreparedStatement()
                .withUUIDValue(root)
                .withStringValue(name)
                .withUUIDValue(newResourceId)
                .execute();

        keyspace
                .prepareQuery(CassandraUtils.CQL3_CF)
                .withCql("INSERT INTO file (file) VALUES (?)")
                .asPreparedStatement()
                .withUUIDValue(newResourceId)
                .execute();

        return newResourceId;
    }

    public void updateFileInfo(final UUID file, long fileSize) throws ConnectionException {
        keyspace
                .prepareQuery(CassandraUtils.CQL3_CF)
                .withCql("UPDATE file SET size=?, storageType=? WHERE file=?")
                .asPreparedStatement()
                .withLongValue(fileSize)
                .withStringValue("CASSANDRA")
                .withUUIDValue(file)
                .execute();
    }

    public long getFileSize(final UUID file) throws ConnectionException {
        final OperationResult<CqlResult<String, String>> execute = keyspace
                .prepareQuery(CassandraUtils.CQL3_CF)
                .withCql("SELECT size FROM file WHERE file=?")
                .asPreparedStatement()
                .withUUIDValue(file)
                .execute();
        for (Row<String, String> row : execute.getResult().getRows()) {
            final Long fileSize = row.getColumns().getLongValue("size", null);
            if (fileSize != null) {
                return fileSize;
            }
        }

        throw new RuntimeException("Unable to find given file");
    }
}
