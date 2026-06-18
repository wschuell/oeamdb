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


class File(Base):
    """
    File class
    """
    __tablename__ = "_file"

    id = Column(Integer, primary_key=True)
    inserted_at = Column(DateTime, server_default=func.current_timestamp())
    name = Column(String)
    filehash = Column(String, unique=True)


class Company(Base):
    """
    Company class
    """
    __tablename__ = "company"

    id = Column(Integer, primary_key=True)
    full_text = Column(String,unique=True)
    name = Column(String)
    address = Column(String)
    inserted_at = Column(DateTime, server_default=func.current_timestamp())

class Product(Base):
    """
    Product class
    """
    __tablename__ = "product"

    id = Column(Integer, primary_key=True)
    product_key = Column(String, unique=True)
    inserted_at = Column(DateTime, server_default=func.current_timestamp())
    name = Column(String)
    shortage = Column(Boolean)
    # approval_holder = Column(String)
    approval_holder = Column(ForeignKey(Company.id))
    requires_prescription = Column(Boolean)
    mrp_dcp = Column(String)

class Substance(Base):
    """
    Substance class
    """
    __tablename__ = "substance"

    id = Column(Integer, primary_key=True)
    inserted_at = Column(DateTime, server_default=func.current_timestamp())
    pubchem_id = Column(BigInteger)
    chembl_id = Column(BigInteger)
    name_de = Column(String,unique=True)
    name_en = Column(String)


class ProductSubstances(Base):

    """
    ProductSubstances class
    """
    __tablename__ = "product_substances"

    inserted_at = Column(DateTime, server_default=func.current_timestamp())
    product_id = Column(ForeignKey(Product.id), primary_key=True)
    substance_id = Column(ForeignKey(Substance.id), primary_key=True)