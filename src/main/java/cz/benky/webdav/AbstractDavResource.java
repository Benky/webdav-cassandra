package cz.benky.webdav;

import cz.benky.webdav.service.CassandraService;
import cz.benky.webdav.service.FileStorageService;
import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.webdav.DavCompliance;
import org.apache.jackrabbit.webdav.DavConstants;
import org.apache.jackrabbit.webdav.DavException;
import org.apache.jackrabbit.webdav.DavResource;
import org.apache.jackrabbit.webdav.DavResourceFactory;
import org.apache.jackrabbit.webdav.DavResourceLocator;
import org.apache.jackrabbit.webdav.DavSession;
import org.apache.jackrabbit.webdav.MultiStatusResponse;
import org.apache.jackrabbit.webdav.lock.ActiveLock;
import org.apache.jackrabbit.webdav.lock.LockInfo;
import org.apache.jackrabbit.webdav.lock.LockManager;
import org.apache.jackrabbit.webdav.lock.Scope;
import org.apache.jackrabbit.webdav.lock.SimpleLockManager;
import org.apache.jackrabbit.webdav.lock.Type;
import org.apache.jackrabbit.webdav.property.DavProperty;
import org.apache.jackrabbit.webdav.property.DavPropertyName;
import org.apache.jackrabbit.webdav.property.DavPropertySet;
import org.apache.jackrabbit.webdav.property.DefaultDavProperty;
import org.apache.jackrabbit.webdav.property.PropEntry;

import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static cz.benky.webdav.util.PathUtils.getParentDirectory;

public abstract class AbstractDavResource implements DavResource {
    private static final String COMPLIANCE_CLASS =
            DavCompliance.concatComplianceClasses(new String[]{DavCompliance._2_});

    private static final String SUPPORTED_METHODS
            = "OPTIONS, GET, HEAD, POST, TRACE, MKCOL, COPY, PUT, DELETE, MOVE, PROPFIND";

    private final DavResourceLocator locator;
    private final DavSession davSession;
    private final CassandraDavResourceFactory factory;
    private final FileStorageService fileStorageService;
    private final CassandraService cassandraService;

    private final String path;

    private LockManager lockManager = new SimpleLockManager();

    private DavPropertySet properties = null;

    public AbstractDavResource(final CassandraDavResourceFactory factory,
                               final DavResourceLocator locator,
                               final DavSession davSession,
                               final CassandraService cassandraService,
                               final FileStorageService fileStorageService) {
        this.factory = factory;
        this.locator = locator;
        this.davSession = davSession;
        this.cassandraService = cassandraService;
        this.fileStorageService = fileStorageService;

        String pathStr = StringUtils.chomp(locator.getResourcePath(), "/");

        if (StringUtils.isEmpty(pathStr)) {
            pathStr = "/";
        }
        this.path = URLDecoder.decode(pathStr);
    }


    @Override
    public String getComplianceClass() {
        return COMPLIANCE_CLASS;
    }

    @Override
    public String getSupportedMethods() {
        return SUPPORTED_METHODS;
    }

    @Override
    public boolean exists() {
        return cassandraService.resourceExists(getPath());
    }

    @Override
    public String getDisplayName() {
        return null;
    }

    @Override
    public DavResourceLocator getLocator() {
        return locator;
    }

    @Override
    public String getResourcePath() {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String getHref() {
        return getPath();
    }

    @Override
    public long getModificationTime() {
        return new Date().getTime();
    }

    @Override
    public DavPropertyName[] getPropertyNames() {
        initProperties();
        return properties.getPropertyNames();
    }

    @Override
    public DavProperty<?> getProperty(DavPropertyName name) {
        initProperties();
        return properties.get(name);
    }

    @Override
    public DavPropertySet getProperties() {
        initProperties();
        return properties;
    }

    @Override
    public void setProperty(DavProperty<?> davProperty) throws DavException {
        initProperties();
    }

    @Override
    public void removeProperty(DavPropertyName name) throws DavException {
        initProperties();
        properties.remove(name);
    }

    @Override
    public MultiStatusResponse alterProperties(List<? extends PropEntry> propEntries) throws DavException {
        return null;
    }

    @Override
    public DavResource getCollection() {
        final DavResourceLocator newLocator = locator.getFactory()
                .createResourceLocator(locator.getPrefix(), getParentDirectory(getPath()));
        try {
            return factory.createResource(newLocator, getSession());
        } catch (DavException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void move(DavResource davResource) throws DavException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void copy(DavResource davResource, boolean b) throws DavException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean isLockable(Type type, Scope scope) {
        return false;
    }

    @Override
    public boolean hasLock(Type type, Scope scope) {
        return false;
    }

    @Override
    public ActiveLock getLock(Type type, Scope scope) {
        return lockManager.getLock(type, scope, this);
    }

    @Override
    public ActiveLock[] getLocks() {
        return new ActiveLock[0];
    }

    @Override
    public ActiveLock lock(LockInfo lockInfo) throws DavException {
        return lockManager.createLock(lockInfo, this);
    }

    @Override
    public ActiveLock refreshLock(LockInfo lockInfo, String s) throws DavException {
        return lockManager.refreshLock(lockInfo, s, this);
    }

    @Override
    public void unlock(String s) throws DavException {
    }

    @Override
    public void addLockManager(LockManager lockManager) {
        this.lockManager = lockManager;
    }

    @Override
    public DavResourceFactory getFactory() {
        return factory;
    }

    @Override
    public DavSession getSession() {
        return davSession;
    }

    public String getPath() {
        return path;
    }


    private void initProperties() {
        if (properties != null) {
            return;
        }
        this.properties = createProperties();
    }

    protected DavPropertySet createProperties() {
        final DavPropertySet localProperties = new DavPropertySet();
        SimpleDateFormat simpleFormat = (SimpleDateFormat) DavConstants.modificationDateFormat.clone();
        simpleFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        localProperties.add(new DefaultDavProperty(DavPropertyName.GETLASTMODIFIED, simpleFormat.format(new Date())));

        if (getDisplayName() != null) {
            localProperties.add(new DefaultDavProperty(DavPropertyName.DISPLAYNAME, getDisplayName()));
        }

        return localProperties;
    }

    public CassandraService getCassandraService() {
        return cassandraService;
    }

    public FileStorageService getFileStorageService() {
        return fileStorageService;
    }
}
