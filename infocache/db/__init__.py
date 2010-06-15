import sqlalchemy as sa
import sqlalchemy.orm as orm
import meta
import ng_schema

def init_model(engine):
    """ Call me before using any of the tables or classes in the model """
    
    meta.engine = engine
    meta.metadata.bind = engine
    meta.metadata.create_all(checkfirst=True)
    meta.Session = orm.sessionmaker(autoflush=True, autocommit=False, bind=engine)

