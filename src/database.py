from sqlalchemy import create_engine, Column, String, Integer, Text, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from datetime import datetime

# Database Configuration
DB_USER = os.getenv('DB_USER', 'user')
DB_PASS = os.getenv('DB_PASS', 'password')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'reviews_db')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class ProcessedReview(Base):
    __tablename__ = 'processed_reviews'

    review_id = Column(String(255), primary_key=True, index=True)
    product_id = Column(String(255), nullable=False)
    user_id = Column(String(255), nullable=False)
    rating = Column(Integer, nullable=False)
    comment = Column(Text, nullable=False)
    sentiment = Column(String(50), nullable=False)
    processed_timestamp = Column(DateTime, default=datetime.utcnow)

def init_db():
    """Creates the database tables."""
    Base.metadata.create_all(bind=engine)

def get_db_session():
    """Provides a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
