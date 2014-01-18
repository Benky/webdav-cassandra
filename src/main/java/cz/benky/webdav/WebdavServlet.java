package cz.benky.webdav;

import cz.benky.webdav.dao.CassandraDao;
import cz.benky.webdav.dao.CassandraFileDao;
import org.apache.jackrabbit.webdav.DavLocatorFactory;
import org.apache.jackrabbit.webdav.DavSessionProvider;
import org.apache.jackrabbit.webdav.simple.LocatorFactoryImpl;
import org.apache.jackrabbit.webdav.simple.LocatorFactoryImplEx;
import org.apache.jackrabbit.webdav.simple.SimpleWebdavServlet;

import javax.jcr.Repository;

public class WebdavServlet extends SimpleWebdavServlet {

    private DavLocatorFactory locatorFactory;

    public WebdavServlet() {
        super();
        setDavSessionProvider(new EmptyDavSessionProvider());
        setResourceFactory(new CassandraDavResourceFactory(new CassandraDao(), new CassandraFileDao()));
    }

    @Override
    public DavLocatorFactory getLocatorFactory() {
        if (locatorFactory == null) {
            locatorFactory = new LocatorFactoryImpl(getPathPrefix());
        }
        return locatorFactory;

    }

    @Override
    public Repository getRepository() {
        // won't be necessary
        return null;
    }

}