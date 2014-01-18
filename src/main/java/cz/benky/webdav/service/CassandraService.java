package cz.benky.webdav.service;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import cz.benky.webdav.dao.CassandraDao;
import cz.benky.webdav.util.PathUtils;

import java.util.List;
import java.util.UUID;

public class CassandraService {
    private final CassandraDao cassandraDao;

    public CassandraService(final CassandraDao cassandraDao) {
        this.cassandraDao = cassandraDao;
    }

    public List<String> getSiblings(final String path) {
        return cassandraDao.getSiblings(path);
    }

    public void createDirectory(final String path) {
        cassandraDao.createDirectory(path);
    }

    public boolean resourceExists(final String path) {
        return cassandraDao.resourceExists(path);
    }

    public UUID getFile(final String localPath) {
        return cassandraDao.getFile(localPath);
    }

    public boolean isFile(final String localPath) {
        return cassandraDao.isFile(localPath);
    }

    public void deleteDirectory(final String path) {
        final String parentDirectory = PathUtils.getParentDirectory(path);
        final UUID file = cassandraDao.getFile(path);
        final UUID parent = cassandraDao.getFile(parentDirectory);
        final String fileName = PathUtils.getFileName(path);

        try {
            cassandraDao.deleteFromDirectory(parent, fileName);
            cassandraDao.deleteFromDirectory(file, ".");
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }
}
