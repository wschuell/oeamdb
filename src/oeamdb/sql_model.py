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
    Date,
    LargeBinary,
    JSON,
    Index,
    and_,
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.dialects.postgresql import JSONB

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
    full_text = Column(String, unique=True)
    name = Column(String)
    address = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    geoloc_success = Column(Boolean)
    json_location = Column(JSON().with_variant(JSONB, "postgresql"))
    inserted_at = Column(DateTime, server_default=func.current_timestamp())


class Product(Base):
    """
    Product class
    """

    __tablename__ = "product"

    id = Column(Integer, primary_key=True)
    product_key = Column(String, unique=True)
    inserted_at = Column(DateTime, server_default=func.current_timestamp())
    updated_at = Column(DateTime, server_default=func.current_timestamp())
    name = Column(String)
    shortage = Column(Boolean)
    approval_holder = Column(ForeignKey(Company.id))
    requires_prescription = Column(Boolean)
    mrp_dcp = Column(String)
    human_usage = Column(Boolean)
    vet_usage = Column(Boolean)
    orig_category = Column(String)
    category = Column(String)
    approval_date = Column(Date)
    atc_code = Column(String)


class Substance(Base):
    """
    Substance class
    """

    __tablename__ = "substance"

    id = Column(Integer, primary_key=True)
    inserted_at = Column(DateTime, server_default=func.current_timestamp())
    pubchem_cid = Column(String)
    pubchem_sid = Column(String)
    chembl_id = Column(String)
    name_en = Column(String, unique=True)
    name_de = Column(String)
    chembl_name = Column(String)
    canonical_smiles = Column(String)
    standard_inchi = Column(String)
    standard_inchi_key = Column(String)
    mol_type = Column(String)
    struct_type = Column(String)

    __table_args__ = (
        Index(
            's_chemblid_idx',
            chembl_id
        ),
        Index(
            's_pubchemcid_idx',
            pubchem_cid
        ),
        Index(
            's_pubchemsid_idx',
            pubchem_sid
        ),
        )

class ProductSubstances(Base):

    """
    ProductSubstances class
    """

    __tablename__ = "product_substances"

    inserted_at = Column(DateTime, server_default=func.current_timestamp())
    product_id = Column(ForeignKey(Product.id), primary_key=True)
    substance_id = Column(ForeignKey(Substance.id), primary_key=True)


    __table_args__ = (
        Index(
            'p_s_reverse_idx',
            substance_id,
            product_id,
        ),
        )

class ATCCode(Base):
    """
    ATCCode class
    """

    __tablename__ = "atc_code"

    atc_code = Column(String, primary_key=True)
    who_name = Column(String)
    level1 = Column(String)
    level2 = Column(String)
    level3 = Column(String)
    level4 = Column(String)
    level5 = Column(String)
    level1_description = Column(String)
    level2_description = Column(String)
    level3_description = Column(String)
    level4_description = Column(String)
    has_been_replaced = Column(Boolean)
    replacement_year = Column(Integer)
    inserted_at = Column(DateTime, server_default=func.current_timestamp())


class ProductATC(Base):

    """
    ProductATC class
    """

    __tablename__ = "product_atc"

    inserted_at = Column(DateTime, server_default=func.current_timestamp())
    product_id = Column(ForeignKey(Product.id), primary_key=True)
    atc_code = Column(ForeignKey(ATCCode.atc_code), primary_key=True)

    __table_args__ = (
        Index(
            'p_atc_reverse_idx',
            atc_code,
            product_id,
        ),
        )

class SubstanceATC(Base):

    """
    SubstanceATC class
    """

    __tablename__ = "substance_atc"

    inserted_at = Column(DateTime, server_default=func.current_timestamp())
    substance_id = Column(ForeignKey(Substance.id), primary_key=True)
    atc_code = Column(ForeignKey(ATCCode.atc_code), primary_key=True)
    notes = Column(String)

    __table_args__ = (
        Index(
            's_atc_reverse_idx',
            atc_code,
            substance_id,
        ),
        )

class Document(Base):
    """
    Document class
    """

    __tablename__ = "document"
    id = Column(Integer, primary_key=True)
    inserted_at = Column(DateTime, server_default=func.current_timestamp())
    doc_type = Column(String)
    product_id = Column(ForeignKey(Product.id))
    url = Column(String)
    valid_since = Column(Date)
    text_content = Column(String)
    download_success = Column(Boolean)
    corrupted_pdf = Column(Boolean)

    __table_args__ = (
        UniqueConstraint(
            "product_id", "doc_type", "valid_since", name="doc_constraint"
        ),
        Index(
            'doc_content_null_idx',
            product_id,
            doc_type,
            valid_since,
            postgresql_where=download_success.is_(None),
        ),
        Index(
            'doc_url_partial_idx',
            url,
            postgresql_where=download_success.is_(None),
        ),
        Index(
            'doc_url_idx',
            url,
        ),
    )


class Course(Base):
    """
    Course class
    """

    __tablename__ = "course"
    id = Column(Integer, primary_key=True)
    taught_by = Column(String)
    level = Column(String)
    semester = Column(String)
    title = Column(String)
    inserted_at = Column(DateTime, server_default=func.current_timestamp())

    __table_args__ = (
        UniqueConstraint("title", "level", "semester", name="course_constraint"),
    )


class CourseMaterial(Base):
    """
    CourseMaterial class
    """

    __tablename__ = "course_material"
    id = Column(Integer, primary_key=True)
    course_id = Column(ForeignKey(Course.id))
    raw_info = Column(JSON().with_variant(JSONB, "postgresql"))
    submitted_by = Column(String)
    taught_by = Column(String)
    level = Column(String)
    semester = Column(String)
    title = Column(String)
    atc_code = Column(ForeignKey(ATCCode.atc_code))
    substance_id = Column(ForeignKey(Substance.id))
    product_id = Column(ForeignKey(Product.id))
    inserted_at = Column(DateTime, server_default=func.current_timestamp())

    __table_args__ = (
        UniqueConstraint("course_id", "raw_info", name="course_mat_constraint"),
    )


class CategoryCorrection(Base):
    """
    CategoryCorrection class
    """

    __tablename__ = "category_correction"
    product_key = Column(String, primary_key=True)
    submitted_by = Column(String)
    submitted_at = Column(DateTime)
    inserted_at = Column(DateTime, server_default=func.current_timestamp())
    new_category = Column(String)


class ATCCorrection(Base):
    """
    ATCCorrection class
    """

    __tablename__ = "atc_correction"
    submitted_by = Column(String)
    name = Column(String)
    replacement_year = Column(Integer)
    inserted_at = Column(DateTime, server_default=func.current_timestamp())
    old_code = Column(String, primary_key=True)
    new_code = Column(String, primary_key=True)
    footnotes = Column(JSON().with_variant(JSONB, "postgresql"))
