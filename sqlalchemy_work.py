from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy .ext.declarative import declarative_base

Base = declarative_base()

class User(Base):

    __tablename__ = "user"

    id = Column('id', Integer, primary_key=True)
    username = Column ('username', String(50), unique = True)


engine = create_engine('mysql://tickets_user:tickets_pass@ticketsdb.cxrz9l1i58ux.us-west-2.rds.amazonaws.com:3306/tickets_db', echo=True)
Base.metadata.create_all(bind=engine)
