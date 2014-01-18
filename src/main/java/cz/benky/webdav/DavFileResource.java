package cz.benky.webdav;

import cz.benky.webdav.service.CassandraService;
import cz.benky.webdav.service.FileStorageService;
import org.apache.jackrabbit.webdav.DavException;
import org.apache.jackrabbit.webdav.DavResource;
import org.apache.jackrabbit.webdav.DavResourceIterator;
import org.apache.jackrabbit.webdav.DavResourceIteratorImpl;
import org.apache.jackrabbit.webdav.DavResourceLocator;
import org.apache.jackrabbit.webdav.DavServletResponse;
import org.apache.jackrabbit.webdav.DavSession;
import org.apache.jackrabbit.webdav.io.InputContext;
import org.apache.jackrabbit.webdav.io.OutputContext;
import org.apache.jackrabbit.webdav.property.DavPropertyName;
import org.apache.jackrabbit.webdav.property.DavPropertySet;
import org.apache.jackrabbit.webdav.property.DefaultDavProperty;
import org.apache.jackrabbit.webdav.property.ResourceType;

import java.io.IOException;
import java.util.Collections;

public class DavFileResource extends AbstractDavResource {

    public DavFileResource(CassandraDavResourceFactory factory,
                           DavResourceLocator locator,
                           DavSession davSession,
                           CassandraService cassandraService,
                           FileStorageService fileStorageService) {
        super(factory, locator, davSession, cassandraService, fileStorageService);
    }

    /**
     * Returns false.
     */
    public boolean isCollection() {
        return false;
    }

    /**
     * Method is not allowed.
     */
    public void addMember(DavResource resource, InputContext inputContext) throws DavException {
        throw new DavException(DavServletResponse.SC_METHOD_NOT_ALLOWED, "Cannot add members to a non-collection resource");
    }

    /**
     * Always returns an empty iterator for a non-collection resource might
     * not have internal members.
     */
    public DavResourceIterator getMembers() {
        return new DavResourceIteratorImpl(Collections.<DavResource>emptyList());
    }

    /**
     * Method is not allowed.
     */
    public void removeMember(DavResource member) throws DavException {
        throw new DavException(DavServletResponse.SC_METHOD_NOT_ALLOWED, "Cannot remove members from a non-collection resource");
    }

    @Override
    public void spool(OutputContext outputContext) throws IOException {
        if (!isCollection()) {
            getFileStorageService().readFile(getPath(), outputContext.getOutputStream());
        }
    }

    @Override
    protected DavPropertySet createProperties() {
        final DavPropertySet properties = super.createProperties();

        properties.add(new DefaultDavProperty(DavPropertyName.GETCONTENTLENGTH, getFileStorageService().getFileSize(getPath())));
        properties.add(new ResourceType(ResourceType.DEFAULT_RESOURCE));
        properties.add(new DefaultDavProperty(DavPropertyName.ISCOLLECTION, "0"));

        return properties;
    }
}
