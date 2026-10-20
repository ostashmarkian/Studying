from typing import Annotated
from fastapi import FastAPI, HTTPException, Path, Query, status
from pydantic import BaseModel, Field
 
app = FastAPI(title="Items API")
 

class ItemCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    price: float = Field(gt=0)
    description: str | None = None
    in_stock: bool = True

class Item(ItemCreate):
    id: int

items_db: dict[int, Item] = {}
next_id = 1

@app.post("/items", response_model=Item, status_code=status.HTTP_201_CREATED)
def create_item(item: ItemCreate):
    global next_id
    new_item = Item(id=next_id, **item.model_dump())
    items_db[next_id] = new_item
    next_id += 1
    return new_item

@app.get("/items", response_model=list[Item])
def list_items(
    skip: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
    in_stock: bool | None = None,
):
    result = list(items_db.values())
    if in_stock is not None:
        result = [i for i in result if i.in_stock == in_stock]
    return result[skip : skip + limit]

@app.get("/items/{item_id}", response_model=Item)
def get_item(item_id: Annotated[int, Path(ge=1)]):
    item = items_db.get(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return item

@app.delete("/items/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_item(item_id: Annotated[int, Path(ge=1)]):
    if item_id not in items_db:
        raise HTTPException(status_code=404, detail="Item not found")
    del items_db[item_id]

