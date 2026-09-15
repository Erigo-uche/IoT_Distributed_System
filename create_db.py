from database import engine
from models import Base

print("DATABASE:", engine.url)
print("REGISTERED TABLES:", Base.metadata.tables.keys())

Base.metadata.create_all(bind=engine)

print("Database tables created!!")