import sqlalchemy as sa
import sqlalchemy.orm as orm
import mon_meta
import ng_schema

def init_model(connection_endpoint):
    """ Call me before using any of the tables or classes in the model """
    engine = sa.create_engine(connection_endpoint)
    mon_meta.metadata.bind = engine
    mon_meta.metadata.create_all()

    mon_meta.engine = engine
    mon_meta.Session = orm.sessionmaker(autoflush=True, autocommit=False, bind=engine)

