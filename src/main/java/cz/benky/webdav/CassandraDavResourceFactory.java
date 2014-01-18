package cz.benky.webdav;

import cz.benky.webdav.dao.CassandraDao;
import cz.benky.webdav.dao.CassandraFileDao;
import cz.benky.webdav.service.CassandraService;
import cz.benky.webdav.service.FileStorageService;
import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.webdav.DavException;
import org.apache.jackrabbit.webdav.DavMethods;
import org.apache.jackrabbit.webdav.DavResource;
import org.apache.jackrabbit.webdav.DavResourceFactory;
import org.apache.jackrabbit.webdav.DavResourceLocator;
import org.apache.jackrabbit.webdav.DavServletRequest;
import org.apache.jackrabbit.webdav.DavServletResponse;
import org.apache.jackrabbit.webdav.DavSession;

import java.net.URLDecoder;
import java.util.UUID;

public class CassandraDavResourceFactory implements DavResourceFactory {

    private final FileStorageService fileStorageService;
    private final CassandraService cassandraService;

    public CassandraDavResourceFactory(final CassandraDao cassandraDao,
                                       final CassandraFileDao cassandraFileDao) {
        this.fileStorageService = new FileStorageService(cassandraDao, cassandraFileDao);
        this.cassandraService = new CassandraService(cassandraDao);
    }

    @Override
    public DavResource createResource(DavResourceLocator locator,
                                      DavServletRequest request,
                                      DavServletResponse response) throws DavException {
        return createResource(locator, request.getDavSession(), request);
    }

    @Override
    public DavResource createResource(DavResourceLocator locator, DavSession davSession) throws DavException {
        return createResource(locator, davSession, null);
    }

    private DavResource createResource(DavResourceLocator locator, DavSession davSession, DavServletRequest request) throws DavException {
        final String localPath = getPath(locator);
        final UUID file = cassandraService.getFile(localPath);

        if (file == null) {
            final boolean isCreateCollection;
            isCreateCollection = request != null && DavMethods.isCreateCollectionRequest(request);
            return new DavNullResource(this, locator, davSession, cassandraService, fileStorageService, isCreateCollection);
        } else {
            if (cassandraService.isFile(localPath)) {
                return new DavFileResource(this, locator, davSession, cassandraService, fileStorageService);
            } else {
                return new DavCollectionResource(this, locator, davSession, cassandraService, fileStorageService);
            }
        }
    }

    private String getPath(DavResourceLocator locator) {
        String pathStr = StringUtils.chomp(locator.getResourcePath(), "/");

        if (StringUtils.isEmpty(pathStr)) {
            pathStr = "/";
        }
        return URLDecoder.decode(pathStr);
    }

}
