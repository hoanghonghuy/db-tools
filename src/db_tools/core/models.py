from sqlalchemy import (Boolean, Column, DateTime, Float, ForeignKey, Integer,
                        String)
from sqlalchemy.orm import declarative_base, relationship

# Base class mà tất cả các model sẽ kế thừa
Base = declarative_base()

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    email = Column(String(255), unique=True)
    is_active = Column(Boolean)
    created_at = Column(DateTime)

    # để chuẩn bị cho tính năng "seeding quan hệ" sau này nó định nghĩa mối quan hệ: một User có thể có nhiều Order
    orders = relationship("Order", back_populates="user")

class Order(Base):
    __tablename__ = 'orders'

    id = Column(Integer, primary_key=True)
    customer_name = Column(String(255), nullable=False)
    shipping_address = Column(String)
    amount = Column(Float)
    order_date = Column(DateTime)
    
    # định nghĩa mối quan hệ ngược lại: một Order thuộc về một User
    user_id = Column(Integer, ForeignKey('users.id'))
    user = relationship("User", back_populates="orders")