package cz.benky.webdav.service;

import com.google.common.io.CountingInputStream;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import cz.benky.webdav.dao.CassandraDao;
import cz.benky.webdav.dao.CassandraFileDao;
import cz.benky.webdav.util.PathUtils;
import org.apache.jackrabbit.webdav.DavException;
import org.apache.jackrabbit.webdav.DavServletResponse;
import org.apache.jackrabbit.webdav.io.InputContext;

import java.io.OutputStream;
import java.util.UUID;

import static cz.benky.webdav.util.PathUtils.getParentDirectory;

public class FileStorageService {
    private final CassandraDao cassandraDao;
    private final CassandraFileDao cassandraFileDao;

    public FileStorageService(final CassandraDao cassandraDao, final CassandraFileDao cassandraFileDao) {
        this.cassandraDao = cassandraDao;
        this.cassandraFileDao = cassandraFileDao;
    }

    public void deleteFile(final String path) {
        final UUID file = cassandraDao.getFile(path);
        final String parentDirectory = PathUtils.getParentDirectory(path);
        final String fileName = PathUtils.getFileName(path);
        final UUID parent = cassandraDao.getFile(parentDirectory);

        // remove possible file content
        try {
            cassandraDao.deleteFromDirectory(parent, fileName);
            cassandraFileDao.deleteFile(file);
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    public void readFile(String fileName, OutputStream os) {
        final UUID file = cassandraDao.getFile(fileName);

        cassandraFileDao.readFile(file.toString(), os);
    }

    public long getFileSize(String fileName) {
        try {
            return cassandraFileDao.getFileSize(cassandraDao.getFile(fileName));
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    public void createFile(final String fullFilePath, final InputContext inputContext) throws DavException {
        if (cassandraDao.getFile(fullFilePath) != null) {
            throw new DavException(DavServletResponse.SC_CONFLICT);
        }
        final String parentDirectory = getParentDirectory(fullFilePath);
        final String fileName = PathUtils.getFileName(fullFilePath);
        final UUID parentId = cassandraDao.getFile(parentDirectory);
        try {
            final UUID fileUUID = cassandraFileDao.createFile(parentId, fileName);
            if (inputContext.hasStream() && inputContext.getContentLength() >= 0) {

                final CountingInputStream countingInputStream = new CountingInputStream(inputContext.getInputStream());
                cassandraFileDao.writeFile(fileUUID, countingInputStream);
                cassandraFileDao.updateFileInfo(fileUUID, countingInputStream.getCount());
            }
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

}
