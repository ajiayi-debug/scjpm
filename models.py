from typing import Optional, List # Supports for type hints
from pydantic import BaseModel # Most widely used data validation library for python
from enum import Enum # Supports for enumerations

# enum for gender
class Gender(str, Enum):
    male = "male"
    female = "female"

# enum for role
class Role(str, Enum):
    admin = "admin"
    user = "user"
    student = "student"
    teacher = "teacher"

class User(BaseModel):
    username: str
    first_name: str
    last_name: str
    middle_name: Optional[str] = None  # Make middle name optional
    gender: Gender
    email_address: Optional[str] = None
    phone_number: str
    disabled: Optional[bool] = None
    roles: List[Role]
    password: str  # Password field added


