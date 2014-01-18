package cz.benky.webdav.dao;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import cz.benky.webdav.util.CassandraUtils;
import cz.benky.webdav.util.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static cz.benky.webdav.util.PathUtils.removeFirstSlash;
import static cz.benky.webdav.util.CassandraUtils.ROOT_UUID;

public class CassandraDao {
    protected static final String CURRENT_DIR = ".";

    private final Keyspace keyspace;

    public CassandraDao() {
        this.keyspace = CassandraUtils.getConnection();
    }

    public boolean isFile(String path) {
        try {
            final UUID entryId = getResourceUUID(ROOT_UUID, path);
            if (entryId == null) {
                return false;
            }

            final OperationResult<CqlResult<String, String>> execute = keyspace
                    .prepareQuery(CassandraUtils.CQL3_CF)
                    .withCql("SELECT file FROM file WHERE file=?")
                    .asPreparedStatement()
                    .withUUIDValue(entryId)
                    .execute();

            return !execute.getResult().getRows().isEmpty();
        } catch (ConnectionException ignore) {
            return false;
        }
    }

    public List<String> getSiblings(final String path) {
        final List<String> result = new LinkedList<String>();

        try {
            final UUID resourceUUID = getResourceUUID(ROOT_UUID, path);
            final OperationResult<CqlResult<String, String>> execute = keyspace
                    .prepareQuery(CassandraUtils.CQL3_CF)
                    .withCql("SELECT child FROM directory WHERE pathId=?")
                    .asPreparedStatement()
                    .withUUIDValue(resourceUUID)
                    .execute();

            for (Row<String, String> row : execute.getResult().getRows()) {
                final String fileName = row.getColumns().getStringValue("child", null);
                if (CURRENT_DIR.equals(fileName)) {
                    continue;
                }
                result.add(fileName);
            }

            return result;
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean resourceExists(String path) {
        try {
            return getResourceUUID(ROOT_UUID, path) != null;
        } catch (ConnectionException ignore) {
            return false;
        }
    }


    private UUID createSubdirectory(UUID root, String name) throws ConnectionException {
        final UUID newResourceId = UUID.randomUUID();

        createEntityInDirectory(root, name, newResourceId);
        createEntityInDirectory(newResourceId, ".", newResourceId);

        return newResourceId;
    }

    public void createEntityInDirectory(UUID root, String name, UUID newResourceId) throws ConnectionException {
        keyspace
                .prepareQuery(CassandraUtils.CQL3_CF)
                .withCql("INSERT INTO directory (pathId, child, childId) VALUES (?, ?, ?)")
                .asPreparedStatement()
                .withUUIDValue(root)
                .withStringValue(name)
                .withUUIDValue(newResourceId)
                .execute();
    }

    public UUID getFile(final String path) {
        try {
            return getResourceUUID(ROOT_UUID, path);
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    public void createDirectory(String path) {
        path = StringUtils.chomp(path, "/");
        if (StringUtils.isEmpty(path)) {
            return;
        }

        try {
            String tmpPath = path;
            UUID rootDir = ROOT_UUID;
            do {
                final String[] directories = StringUtils.split(removeFirstSlash(tmpPath), "/", 2);
                if (StringUtils.isEmpty(ArrayUtils.get(directories, 0))) {
                    break;
                }
                final String directoryName = directories[0];
                final UUID resourceUUID = getResourceUUID(rootDir, directoryName);
                if (resourceUUID != null) {
                    rootDir = resourceUUID;
                } else {
                    createSubdirectory(rootDir, directoryName);
                }
                tmpPath = ArrayUtils.get(directories, 1);
            } while (true);
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
    }


    private UUID getResourceUUID(final UUID root, final String path) throws ConnectionException {
        final String[] split = StringUtils.split(removeFirstSlash(path), "/", 2);
        if (StringUtils.isEmpty(ArrayUtils.get(split, 0))) {
            return root;
        }

        final UUID childId = getChildUUID(root, split[0]);
        if (childId == null) {
            return null;
        } else {
            final String restOfPath = ArrayUtils.get(split, 1);
            return !StringUtils.isEmpty(restOfPath) ? getResourceUUID(childId, restOfPath) : childId;
        }
    }

    private UUID getChildUUID(final UUID root, final String directoryName) throws ConnectionException {
        final OperationResult<CqlResult<String, String>> execute = keyspace
                .prepareQuery(CassandraUtils.CQL3_CF)
                .withCql("SELECT childid FROM directory WHERE pathId=? AND child=?")
                .asPreparedStatement()
                .withUUIDValue(root)
                .withStringValue(directoryName)
                .execute();

        for (Row<String, String> row : execute.getResult().getRows()) {
            return row.getColumns().getUUIDValue("childid", null);
        }
        return null;
    }

    public void deleteFromDirectory(UUID parent, String fileName) throws ConnectionException {
        keyspace
                .prepareQuery(CassandraUtils.CQL3_CF)
                .withCql("DELETE FROM directory WHERE pathId=? AND child=?")
                .asPreparedStatement()
                .withUUIDValue(parent)
                .withStringValue(fileName)
                .execute();
    }


}
