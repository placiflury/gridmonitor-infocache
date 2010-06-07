""" SQLAlchemy Metadata and Session object """

from sqlalchemy import MetaData

__all__ = ['engine', 'metadata', 'Session']  # does not make much sense here...

# SQLAlchemy database engine.  Updated by ch.smscg.infocache.gris.init_model().
engine = None
    
# SQLAlchemy session manager.  Updated by ch.smscg.infocache.init_model().
Session = None
    
# Global metadata. If you have multiple databases with overlapping table 
# names, you'll need a metadata for each database.
metadata = MetaData()


