import sqlalchemy as sa
import sqlalchemy.orm as orm
import meta
import ng_schema

def init_model(connection_endpoint):
    """ Call me before using any of the tables or classes in the model """
    engine = sa.create_engine(connection_endpoint)
    meta.metadata.bind = engine
    meta.metadata.create_all()

    meta.engine = engine
    meta.Session = orm.sessionmaker(autoflush=True, autocommit=False, bind=engine)

