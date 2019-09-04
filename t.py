from sqlalchemy import create_engine, ForeignKey, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

engine = create_engine('sqlite:///sales.db', echo = True)
Base = declarative_base()


class Customer(Base):
   __tablename__ = 'customers'

   id = Column(Integer, primary_key = True)
   name = Column(String)
   address = Column(String)
   email = Column(String)

class Invoice(Base):
   __tablename__ = 'invoices'
   
   id = Column(Integer, primary_key = True)
   custid = Column(Integer, ForeignKey('customers.id'))
   invno = Column(Integer)
   amount = Column(Integer)
   customer = relationship("Customer", back_populates = "invoices")

Customer.invoices = relationship("Invoice", order_by = Invoice.id, back_populates = "customer")
Base.metadata.create_all(engine)