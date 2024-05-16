import logging
import sqlite3
from datetime import datetime

from db.init_db import DB_PATH
from db.base import engine
from db.customer import Customer
from db.item import Item
from db.order_items import OrderItems
from db.orders import Orders

from sqlalchemy import select, text
from sqlalchemy.orm import Session


def execute_query(query, params=None):
    with engine.connect() as conn:
        return conn.execute(text(query), params)


# def execute_query(query, params):
#     with sqlite3.connect(DB_PATH) as conn:
#         cur = conn.cursor()
#         cur.execute(query, params)
#         rows = cur.fetchall()
#         return rows


def execute_insert_query(query, params):
    with engine.connect() as conn:
        cursor = conn.execute(text(query), params)
        result = cursor.fetchone()
        conn.commit()
        return result

    # with sqlite3.connect(DB_PATH) as conn:
    #     cur = conn.cursor()
    #     cur.execute(query, params)
    #     result = cur.fetchone()
    #     conn.commit()
    #     return result


def execute_insert_queries(query, params_tuple):
    with engine.connect() as conn:
        conn.execute(text(query), params_tuple)
        conn.commit()

    # with sqlite3.connect(DB_PATH) as conn:
    #     cur = conn.cursor()
    #     cur.executemany(query, params_tuple)
    #     conn.commit()


def get_customers():
    with Session(engine) as session:
        stmt = select(Customer)
        result = session.execute(stmt)
        customers = result.scalars().all()

        return customers

    # rows = execute_query("SELECT * FROM customer", {})
    # return rows


def get_orders_of_customer(customer_id):
    rows = execute_query(
        """
        SELECT 
            item.name, 
            item.description, 
            order_items.quantity as amount,
            item.price, 
            item.price*order_items.quantity AS total
        FROM orders 
        JOIN order_items 
        ON 
            order_items.order_id = orders.id 
        JOIN item
        ON 
            item.id = order_items.item_id
        WHERE
            orders.customer_id=:customer_id
        """,
        {"customer_id": customer_id},
    )
    return rows


def get_total_cost_of_an_order(order_id):
    rows = execute_query(
        """
        SELECT 
            SUM(item.price*order_items.quantity) AS total
        FROM orders 
        JOIN order_items 
        ON 
            order_items.order_id = orders.id 
        JOIN item 
        ON 
            item.id = order_items.item_id
        WHERE
            orders.id=:order_id
        """,
        {"order_id": order_id},
    )
    # print(rows.one())
    return rows.one().total


def get_orders_between_dates(after, before):
    rows = execute_query(
        """
        SELECT
            customer.name,
            item.name, 
            item.price, 
            item.price*order_items.quantity AS total
        FROM orders 
        JOIN customer
        ON
            customer.id = orders.customer_id
        JOIN order_items 
        ON 
            order_items.order_id = orders.id 
        JOIN item 
        ON 
            item.id = order_items.item_id
        WHERE
            orders.order_time >= :after
        AND
            orders.order_time <= :before
        """,
        {"after": after, "before": before},
    )
    return rows


def add_new_order_for_customer(customer_id, items):
    try:

        with Session(engine) as session:
            result = session.execute(
                select(Customer).where(Customer.id == customer_id)
            )
            customer = result.scalar()

            new_order = Orders(
                customer_id=customer_id,
                order_time=datetime.now(),
                customer=customer,
            )

            new_order.order_items = [
                OrderItems(
                    item_id=item['id'],
                    quantity=item['quantity'],
                )
                for item in items
            ]

            session.add(new_order)
            session.commit()

        return True

    except Exception:
        logging.exception("Failed to add new order")
        return False

# def add_new_order_for_customer(customer_id, items):
#     try:
#         new_order_id = execute_insert_query(
#             """
#             INSERT INTO orders
#                 (customer_id, order_time)
#             VALUES
#                 (:customer_id, Date('now'))
#             RETURNING id
#             """,
#             {"customer_id": customer_id},
#         ).id
#
#         execute_insert_queries(
#             """
#         INSERT INTO order_items
#             (order_id, item_id, quantity)
#         VALUES
#             (:order_id, :item_id, :quantity)
#         """,
#             [
#                 {
#                     "order_id": new_order_id,
#                     "item_id": item["id"],
#                     "quantity": item["quantity"],
#                 }
#                 for item in items
#             ],
#         )
#
#         return True
#
#     except Exception:
#         logging.exception("Failed to add new order")
#         return False
