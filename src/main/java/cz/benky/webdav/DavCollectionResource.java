package cz.benky.webdav;

import cz.benky.webdav.service.CassandraService;
import cz.benky.webdav.service.FileStorageService;
import org.apache.commons.lang.StringUtils;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DavCollectionResource extends AbstractDavResource {
    public DavCollectionResource(final CassandraDavResourceFactory factory,
                                 final DavResourceLocator locator,
                                 final DavSession davSession,
                                 final CassandraService cassandraService,
                                 final FileStorageService fileStorageService) {
        super(factory, locator, davSession, cassandraService, fileStorageService);
    }

    @Override
    public boolean isCollection() {
        return true;
    }

    @Override
    public void spool(OutputContext outputContext) throws IOException {
        // nothing here
    }

    @Override
    public void addMember(DavResource resource, InputContext inputContext) throws DavException {
        final AbstractDavResource cassandraResource = (AbstractDavResource) resource;
        final String destPath = cassandraResource.getPath();
        if (cassandraResource.isCollection()) {
            getCassandraService().createDirectory(destPath);
        } else {
            getFileStorageService().createFile(destPath, inputContext);
        }
    }

    @Override
    public DavResourceIterator getMembers() {
        if (!isCollection()) {
            return new DavResourceIteratorImpl(Collections.<DavResource>emptyList());
        } else {
            ArrayList<DavResource> list = new ArrayList<DavResource>();
            for (String sub : getCassandraService().getSiblings(getPath())) {
                DavResourceLocator resourceLocator = getLocator().getFactory().createResourceLocator(getLocator().getPrefix(),
                        getLocator().getWorkspacePath(),
                        StringUtils.chomp(getPath(), "/") + "/" + sub,
                        false);
                try {
                    list.add(getFactory().createResource(resourceLocator, getSession()));
                } catch (DavException ex) {
                    ex.printStackTrace();
                }

            }
            return new DavResourceIteratorImpl(list);
        }
    }

    @Override
    public void removeMember(DavResource resource) throws DavException {
        final AbstractDavResource cassandraResource = (AbstractDavResource) resource;
        final String path = cassandraResource.getPath();
        if (path.equals("/")) {
            return;
        }

        if (cassandraResource.isCollection()) {
            final List<String> siblings = getCassandraService().getSiblings(path);
            if (!siblings.isEmpty()) {
                throw new DavException(DavServletResponse.SC_CONFLICT, "You cannot delete collection with content");
            }
            getCassandraService().deleteDirectory(path);
        } else {
            getFileStorageService().deleteFile(path);
        }
    }

    @Override
    protected DavPropertySet createProperties() {
        final DavPropertySet properties = super.createProperties();

        properties.add(new ResourceType(ResourceType.COLLECTION));
        properties.add(new DefaultDavProperty(DavPropertyName.ISCOLLECTION, "1"));

        return properties;
    }

}
