from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    ForeignKey,
    UniqueConstraint,
    func,
    Float,
    Boolean,
    Time,
    DateTime,
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


Base = declarative_base()


class Product(Base):
    """
    Product class
    """
    __tablename__ = "product"

    id = Column(BigInteger, primary_key=True)
    inserted_at = Column(DateTime, server_default=func.current_timestamp())
    name = Column(String, unique=True)


class Substance(Base):
    """
    Substance class
    """
    __tablename__ = "substance"

    id = Column(BigInteger, primary_key=True)
    inserted_at = Column(DateTime, server_default=func.current_timestamp())
    pubchem_id = Column(BigInteger)
    chembl_id = Column(BigInteger)
