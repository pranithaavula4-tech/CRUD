from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker, declarative_base
from typing import List

# --- Database setup ---
DATABASE_URL = "sqlite:///./items.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Item(Base):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    description = Column(String, nullable=True)
    quantity = Column(Integer, default=0)
    price = Column(Float, default=0.0)


Base.metadata.create_all(bind=engine)


# --- Pydantic schemas ---
class ItemCreate(BaseModel):
    name: str
    description: str | None = None
    quantity: int = 0
    price: float = 0.0


class ItemResponse(ItemCreate):
    id: int

    class Config:
        from_attributes = True


# --- App ---
app = FastAPI(title="CRUD Demo API")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def root():
    return {"message": "CRUD API is running. Visit /docs for the interactive UI."}


@app.post("/items", response_model=ItemResponse)
def create_item(item: ItemCreate):
    db = SessionLocal()
    db_item = Item(**item.dict())
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    db.close()
    return db_item


@app.get("/items", response_model=List[ItemResponse])
def read_items():
    db = SessionLocal()
    items = db.query(Item).all()
    db.close()
    return items


@app.get("/items/{item_id}", response_model=ItemResponse)
def read_item(item_id: int):
    db = SessionLocal()
    item = db.query(Item).filter(Item.id == item_id).first()
    db.close()
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return item


@app.put("/items/{item_id}", response_model=ItemResponse)
def update_item(item_id: int, updated: ItemCreate):
    db = SessionLocal()
    item = db.query(Item).filter(Item.id == item_id).first()
    if not item:
        db.close()
        raise HTTPException(status_code=404, detail="Item not found")
    for key, value in updated.dict().items():
        setattr(item, key, value)
    db.commit()
    db.refresh(item)
    db.close()
    return item


@app.delete("/items/{item_id}")
def delete_item(item_id: int):
    db = SessionLocal()
    item = db.query(Item).filter(Item.id == item_id).first()
    if not item:
        db.close()
        raise HTTPException(status_code=404, detail="Item not found")
    db.delete(item)
    db.commit()
    db.close()
    return {"message": f"Item {item_id} deleted"}
