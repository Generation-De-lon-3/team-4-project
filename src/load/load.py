from connection import initdb
from branches import branches
from products import products
from orders import orders
from baskets import baskets
from payments import payments


def handle(event, handler):
    
    # data = 
    
    
    initdb()
    branches(data)
    products(data)
    orders(data)
    payments(data)
    baskets(data)
