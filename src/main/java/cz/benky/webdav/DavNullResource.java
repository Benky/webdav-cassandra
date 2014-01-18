package cz.benky.webdav;

import cz.benky.webdav.service.CassandraService;
import cz.benky.webdav.service.FileStorageService;
import org.apache.jackrabbit.webdav.DavException;
import org.apache.jackrabbit.webdav.DavResource;
import org.apache.jackrabbit.webdav.DavResourceIterator;
import org.apache.jackrabbit.webdav.DavResourceIteratorImpl;
import org.apache.jackrabbit.webdav.DavResourceLocator;
import org.apache.jackrabbit.webdav.DavSession;
import org.apache.jackrabbit.webdav.io.InputContext;
import org.apache.jackrabbit.webdav.io.OutputContext;

import java.io.IOException;
import java.util.Collections;

public class DavNullResource extends AbstractDavResource {
    private final boolean collection;

    public DavNullResource(CassandraDavResourceFactory factory,
                           DavResourceLocator locator,
                           DavSession davSession,
                           CassandraService cassandraService,
                           FileStorageService fileStorageService,
                           boolean collection) {
        super(factory, locator, davSession, cassandraService, fileStorageService);
        this.collection = collection;
    }

    @Override
    public boolean isCollection() {
        return collection;
    }

    @Override
    public void spool(OutputContext outputContext) throws IOException {
    }

    @Override
    public void addMember(DavResource resource, InputContext inputContext) throws DavException {
    }

    @Override
    public DavResourceIterator getMembers() {
        return new DavResourceIteratorImpl(Collections.<DavResource>emptyList());
    }

    @Override
    public void removeMember(DavResource member) throws DavException {
    }
}
