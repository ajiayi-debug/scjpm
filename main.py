# import fast api
from fastapi import FastAPI, HTTPException, status, Depends
from fastapi.responses import FileResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from models import * # import the user model defined by us
# imports for the MongoDB database connection
from motor.motor_asyncio import AsyncIOMotorClient
# import for fast api lifespan
from contextlib import asynccontextmanager
from typing import List, Optional # Supports for type hints
from pydantic import BaseModel # Most widely used data validation library for python
from dotenv import load_dotenv
import os
from starlette.middleware.cors import CORSMiddleware
import pandas as pd
from passlib.context import CryptContext  # Password hashing utility
from jose import JWTError, jwt
from datetime import datetime, timedelta

load_dotenv()

MONGO_URI=os.getenv('uri_mongo')

# define a lifespan method for fastapi
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the database connection
    await startup_db_client(app)
    yield
    # Close the database connection
    await shutdown_db_client(app)

# method for start the MongoDb Connection
async def startup_db_client(app):
    app.mongodb_client = AsyncIOMotorClient(
        MONGO_URI)
    app.mongodb = app.mongodb_client.get_database("college")
    print("MongoDB connected.")

# method to close the database connection
async def shutdown_db_client(app):
    app.mongodb_client.close()
    print("Database disconnected.")


# creating a server with python FastAPI
app = FastAPI(lifespan=lifespan)

#To allow frontend to call backend with differing localhosts
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
SECRET_KEY = os.getenv("SECRET_KEY", "secret_key")  # Use environment variable or default
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Function to verify password
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


# Function to hash a password
def get_password_hash(password):
    return pwd_context.hash(password)


# Function to create an access token
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


# Function to get the current user based on the token
async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = await app.mongodb["users"].find_one({"username": username})
    if user is None:
        raise credentials_exception
    return user


# Function to authenticate a user during login
async def authenticate_user(username: str, password: str):
    user = await app.mongodb["users"].find_one({"username": username})
    if not user:
        return False
    if not verify_password(password, user["password"]):
        return False
    return user


# Token endpoint for user login
@app.post("/token", response_model=dict)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = await authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user["username"]}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}



# hello world endpoint
@app.get("/")
def read_root():  # function that is binded with the endpoint
    return {"Hello": "World"}

# C <=== Create
@app.post("/api/v1/create-user", response_model= User)
async def insert_user(user: User):
    user.password = get_password_hash(user.password)  # Hash the password before storing
    result = await app.mongodb["users"].insert_one(user.dict())
    inserted_user = await app.mongodb["users"].find_one({"_id": result.inserted_id})
    return inserted_user

# R <=== Read
# Read all users as a list of json 
# only if admin, then will return all users in db, else []
# current_user depends area is for auth 
@app.get("/api/v1/read-all-users", response_model=List[User])
async def read_users(current_user: User = Depends(get_current_user)):
    if "admin" not in current_user['roles']:
        return []
    users = await app.mongodb["users"].find().to_list(None)
    return users

# Read all users as a dataframe (pd)
@app.get("/api/v1/read-all-users-dataframe")
async def get_users_dataframe(current_user: User = Depends(get_current_user)):
    if "admin" not in current_user['roles']:
        return []
    # Retrieve all users from the MongoDB collection
    users_cursor = app.mongodb["users"].find({})
    users = await users_cursor.to_list(None)

    # Convert MongoDB data to pandas DataFrame
    users_df = pd.DataFrame(users)

    # Optional: Convert ObjectId to string if present
    if "_id" in users_df.columns:
        users_df["_id"] = users_df["_id"].astype(str)

    # Return DataFrame as a JSON-like response
    # Optionally, you can use `to_dict(orient="records")` if you want a more structured JSON
    return users_df.to_dict(orient="records")

#Read all users as excel file
@app.get("/api/v1/users-csv")
async def get_users_csv(current_user: User = Depends(get_current_user)):
    if "admin" not in current_user['roles']:
        return []
    users_cursor = app.mongodb["users"].find({})
    users = await users_cursor.to_list(None)
    users_df = pd.DataFrame(users)
    if "_id" in users_df.columns:
        users_df["_id"] = users_df["_id"].astype(str)
    
    # Save DataFrame as a CSV file
    csv_file_path = "/tmp/users.csv"
    users_df.to_csv(csv_file_path, index=False)
    
    # Return the CSV file as a response
    return FileResponse(csv_file_path, media_type='text/csv', filename="users.csv")

# Read one user by email_address
@app.get("/api/v1/read-user/{email_address}", response_model=User)
async def read_user_by_email(email_address: str, current_user: User = Depends(get_current_user)):
    if "admin" not in current_user['roles']:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have access to perform this operation"
        )
    user = await app.mongodb["users"].find_one({"email_address": email_address})
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user


# U <=== Update
# Update user
 
class UpdateUserDTO(BaseModel):
    other_names: List[str] = None
    age: int = None
    # Include other fields as needed, with defaults to None or use the exclude_unset=True option

@app.put("/api/v1/update-user/{email_address}", response_model=User)
async def update_user(email_address: str, user_update: UpdateUserDTO):
    updated_result = await app.mongodb["users"].update_one(
        {"email_address": email_address}, {"$set": user_update.dict(exclude_unset=True)})
    if updated_result.modified_count == 0:
        raise HTTPException(status_code=404, detail="User not found or no update needed")
    updated_user = await app.mongodb["users"].find_one({"email_address": email_address})
    return updated_user

# D <=== Delete
# Delete user by email_address
@app.delete("/api/v1/delete-user/{email_address}", response_model=dict)
async def delete_user_by_email(email_address: str):
    delete_result = await app.mongodb["users"].delete_one({"email_address": email_address})
    if delete_result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    return {"message": "User deleted successfully"}

@app.get("/api/v1/read-user/{email_address}", response_model=User)
async def read_user_by_email(email_address: str):
    user = await app.mongodb["users"].find_one({"email_address": email_address})
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@app.get("/api/v1/me", response_model=User)
async def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user