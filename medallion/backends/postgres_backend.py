import logging

from sqlalchemy import create_engine, JSON, Column, Integer, String, Boolean, ForeignKey, and_, func, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import DBAPIError

from medallion.filters.postgres_filter import PostgresFilter
from medallion.utils.common import (create_bundle, format_datetime,
                                    generate_status, get_timestamp)

from .base import Backend

# Module-level logger
log = logging.getLogger(__name__)

# SQLAlchemy ORM object
DBBase = declarative_base()


class PostgresBackend(Backend):
    def __init__(self, server=None, username=None, password=None, db=None, **kwargs):
        try:
            engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{server}/{db}")
            DBBase.metadata.bind = engine
            dbsession = sessionmaker()
            dbsession.bind = engine
            self.session = dbsession()
            DBBase.metadata.create_all()

        except DBAPIError as e:
            log.error(f"Database connection error: error connecting to {server}/{db}: {e}")

    def get_collections(self, api_root):
        log.debug(f'Locating all Collections below api_root {api_root}')
        query = self.session.query(Collections)

        resources = []
        for c in query:
            resources.append(c.to_dict())

        return resources

    def get_collection(self, api_root, collection_id):
        log.debug(f'Pulling Collection {api_root}/{collection_id}')
        query = self.session.query(Collections).filter(Collections.id == collection_id).first()

        if not query:
            return

        d = query.to_dict()
        d['media_types'] = d.setdefault('media_types',
                                        'application/vnd.oasis.stix+json; version=2.0').split(sep=',')
        # TODO: Real access control
        d['can_read'] = True
        d['can_write'] = True

        return d

    def get_object_manifest(self, api_root, collection_id, filter_args, allowed_filters):
        log.debug(f'Producing Manifest under /{api_root}/{collection_id}/manifest/')
        manifest = []
        print(manifest)
        maker = {}
        query = self.session.query(Objects.id, Objects.date_added, Objects.modified).all()
        for q in query:
            test = maker.get(q[0], None)
            if not test:
                maker[q[0]] = {
                    'id': q[0],
                    'date_added': q[1],
                    'versions': [q[2]],
                    'media_types': ['application/vnd.oasis.stix+json; version=2.0']
                }
            else:
                maker[q[0]]['versions'].append(q[2])
        for k in maker:
            log.debug(f'Adding {k}')
            manifest.append(maker[k])
        return manifest

    def get_api_root_information(self, api_root):
        log.debug(f'Pulling API Root Information for /{api_root}/')
        # query = select([self.api_root]).where(self.api_root.c.uri == api_root)
        query = self.session.query(APIRoot).filter(APIRoot.uri == api_root).first()

        if query:
            return query.to_dict()

    # TODO: Test get_status
    def get_status(self, api_root, status_id):
        log.debug(f'Getting status {status_id} for API root {api_root}')
        status = self.session.query(Status).filter(Status.id == status_id).first()

        if status:
            s = status.to_dict()
            # Modify the object to product a valid resource
            s["pendings"] = self._split_or_empty(s["pendings"])
            s["failures"] = self._split_or_empty(s["failures"])
            s["successes"] = self._split_or_empty(s["successes"])
            return s

    @staticmethod
    def _split_or_empty(str):
        if str == "":
            return []
        else:
            return str.split(",")

    def get_objects(self, api_root, collection_id, filter_args, allowed_filters):
        log.debug(f'Getting objects under {api_root}/{collection_id}')
        objects = []

        # TODO: matching
        query = self.session.query(Objects.id, Objects.object, func.max(Objects.modified)).group_by(
            Objects.id).order_by(Objects.modified).all()

        for q in query:
            objects.append(q.object)

        bundle = create_bundle(objects)
        return bundle

    def add_objects(self, api_root, id_, objs, request_time):
        log.debug(f'Adding new object(s) to {api_root}/{id_}')
        log.debug(print(objs))

        # TODO: Validate STIX 2.0 bundle
        # TODO: Do not add new versions if object has been revoked

        # Define return results.
        failed = 0
        succeeded = 0
        pending = 0
        # TODO: Background jobs if this is going to take too long(?)
        successes = []
        failures = []

        for new_obj in objs["objects"]:
            try:
                new = Objects(id=new_obj["id"],
                              collection=id_,
                              date_added=request_time,
                              type=new_obj["type"],
                              object=new_obj,
                              created=new_obj["created"],
                              modified=new_obj["modified"])
                self.session.add(new)
                self.session.commit()
                successes.append(new_obj["id"])
            except DBAPIError as e:
                print(e)
                failed += 1
                failures.append(new_obj["id"])
                self.session.rollback()
        status = generate_status(request_time=request_time,
                                 status="complete",
                                 succeeded=succeeded,
                                 failed=failed,
                                 pending=pending,
                                 successes_ids="".join(successes),
                                 failures=",".join(failures))
        self._put_status(status)
        return status

    def get_object(self, api_root, collection_id, object_id, filter_args, allowed_filters):
        objects = []
        # TODO: Implement filtering

        query = self.session.query(Objects).filter(Objects.id == object_id).order_by(desc(Objects.date_added)).all()

        objects.append(query[0].to_dict())
        return create_bundle(objects)

    def server_discovery(self):
        log.debug(f'Accessing TAXII service discovery information.')
        resource = {'title': 'Default discovery title', 'api_roots': []}

        # Title is the only required element of a dictionary resource, so we will define it temporarily
        # TODO: Modify method to pass root URI:
        # TODO: Define default api_root
        # TODO: Define server configuration file/parameters

        # Get API roots
        query = self.session.query(APIRoot.uri)
        for result in query:
            resource['api_roots'].append(f'/{result.uri}/')

        return resource

    def _put_status(self, status):
        s = Status(status)
        self.session.add(s)
        try:
            self.session.commit()
        except DBAPIError as e:
            log.error(e)
            self.session.rollback()


class ObjDict(object):
    def to_dict(self):
        ret_val = {}
        for k, v in self.__dict__.items():
            if not k.startswith('_'):
                ret_val[k] = v
        return ret_val


class Objects(DBBase, ObjDict):
    """
    Objects Table:
        - id: uniquely identifies object <string>
        - type: type of object <string>
        - version: Object version (RFC3339 timestamp) <string>
        - collection: Collection to which this object belongs <string>
        - object: STIX 2.0 object <JSON>
    """
    __tablename__ = 'objects'

    id = Column(String(128), primary_key=True)
    type = Column(String())
    date_added = Column(String(64))
    collection = Column(ForeignKey('collections.id'))
    object = Column(JSON)
    revoked = Column(Boolean, server_default="False")
    created = Column(String())
    modified = Column(String(), primary_key=True)


class Collections(DBBase, ObjDict):
    """
    Collections Table:
        - id: Universally and uniquely identifies this Collection <string>
        - title: Human readable plain text title used to identify this Collection <string>
        - description: Human readable plain text description for this Colleciton <string>
        - media_types: A list of supported media types for Objects in this Collection: [<string>] (use
            comma-delimited string)
        - api_root: API Root to which this Collection belongs <string> (FOREIGN KEY)
    """
    __tablename__ = 'collections'

    id = Column(String(36), primary_key=True)
    title = Column(String(255))
    description = Column(String(255))
    media_types = Column(String(255), server_default='application/vnd.oasis.stix+json')
    api_root = Column(ForeignKey('api_root.uri'))


class APIRoot(DBBase, ObjDict):
    """
    API Root Table:
        - uri: api_root URI string <string> (PRIMARY KEY)
        - title: human readable plain text name used to identify this API instance <string>
        - description: human readable plain text name used for this API Root <string> (optional)
        - versions: The list of TAXII versions that this API root is compatable with [<string>] (use
            comma-delimited string)
        - max_content_length: Max size of the request body in octets the server can support.  <integer>
    """
    __tablename__ = 'api_root'

    uri = Column(String(50), primary_key=True)
    title = Column(String(255))
    description = Column(String(255))
    versions = Column(String(255), server_default='taxii-2.0')
    max_content_length = Column(Integer())


class Status(DBBase, ObjDict):
    """
    Status Table:
        TODO: Implement status table.  NB: TAXII 2.0 Specification requires object statuses for at least 24 hours.
    """

    __tablename__ = 'status'

    id = Column(String(36), primary_key=True)
    status = Column(String(8))
    request_timestamp = Column(String(255))
    total_count = Column(Integer)
    success_count = Column(Integer, default=0)
    failure_count = Column(Integer, default=0)
    pending_count = Column(Integer, default=0)
    successes = Column(String(), default="")
    failures = Column(String(), default="")
    pendings = Column(String(), default="")

    def __init__(self, initial_data):
        for k in initial_data:
            setattr(self, k, initial_data[k])
        log.debug(self.to_dict())
