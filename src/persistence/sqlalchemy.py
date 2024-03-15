from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, UUID, String


Base = declarative_base()


class Project(Base):
    __tablename__ = "projects"

    id = Column(UUID, primary_key=True)
    name = Column(String)
