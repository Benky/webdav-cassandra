package cz.benky.webdav;

import org.apache.jackrabbit.webdav.DavException;
import org.apache.jackrabbit.webdav.DavSessionProvider;
import org.apache.jackrabbit.webdav.WebdavRequest;

/**
 * Implementation of the {@link DavSessionProvider} that does nothing
 */
public class EmptyDavSessionProvider implements DavSessionProvider {

    public boolean attachSession(WebdavRequest request) throws DavException {
        return true;
    }

    public void releaseSession(WebdavRequest request) {
    }
}

