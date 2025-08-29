from typing import Optional
from fastapi import FastAPI, HTTPException, status, Response, Depends
from fastapi.params import Body
from pydantic import BaseModel
import sqlite3
import os

app = FastAPI()

DATABASE_URL = "posts.db"

def get_db():
    conn = sqlite3.connect(DATABASE_URL)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    if not os.path.exists(DATABASE_URL):
        conn = sqlite3.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                published BOOLEAN DEFAULT TRUE,
                rating INTEGER
            )
        ''')
        conn.commit()
        conn.close()
        print("Database initialized successfully")

@app.on_event("startup")
def startup_event():
    init_db()

class Post(BaseModel):
    title: str
    content: str
    published: bool = True
    rating: Optional[int] = None

class PostResponse(Post):
    id: int

    class Config:
        orm_mode = True

@app.get("/")
def root():
    return {"message": "Hello World!!!"}

@app.get("/posts")
def get_posts(db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("SELECT * FROM posts")
    posts = cursor.fetchall()
    return {"data": [dict(post) for post in posts]}

@app.post("/posts", status_code=status.HTTP_201_CREATED)
def create_posts(post: Post, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute('''
        INSERT INTO posts (title, content, published, rating)
        VALUES (?, ?, ?, ?)
    ''', (post.title, post.content, post.published, post.rating))
    db.commit()
    
    cursor.execute("SELECT * FROM posts WHERE id = last_insert_rowid()")
    new_post = cursor.fetchone()
    
    return {"data": dict(new_post)}

@app.get("/posts/{id}")
def get_post(id: int, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("SELECT * FROM posts WHERE id = ?", (id,))
    post = cursor.fetchone()
    
    if not post:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"post with id: {id} was not found")
    
    return {"post_detail": dict(post)}

@app.delete("/posts/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_post(id: int, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("DELETE FROM posts WHERE id = ?", (id,))
    db.commit()
    
    if cursor.rowcount == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"post with id: {id} does not exist")
    
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.put("/posts/{id}")
def update_post(id: int, post: Post, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute('''
        UPDATE posts 
        SET title = ?, content = ?, published = ?, rating = ?
        WHERE id = ?
    ''', (post.title, post.content, post.published, post.rating, id))
    db.commit()
    
    if cursor.rowcount == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"post with id: {id} does not exist")
    
    cursor.execute("SELECT * FROM posts WHERE id = ?", (id,))
    updated_post = cursor.fetchone()
    
    return {"data": dict(updated_post)}