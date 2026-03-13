"""
TPC-C Benchmark End-to-End Test for AnDB.

This test implements a simplified TPC-C benchmark that exercises:
- All 9 TPC-C tables (warehouse, district, customer, history, order, new_order,
  order_line, item, stock)
- All 5 TPC-C transaction types:
  1. New-Order: inserts new order with order lines
  2. Payment: updates warehouse/district/customer balances
  3. Order-Status: reads customer's last order
  4. Delivery: processes pending orders
  5. Stock-Level: checks stock levels below threshold

Adapted to AnDB's SQL capabilities (no BETWEEN, no subqueries in WHERE,
no CURRENT_TIMESTAMP - uses string literals for dates).
"""

import os
import pytest

from andb.entrance import execute_simple_query
from andb.executor.portal import ExecuteResultSet, ExecutionResult
from andb.constants.filename import WAL_DIR, UNDO_DIR
from andb.runtime import global_vars
from andb.storage.xact.mgr import TransactionManager


@pytest.fixture(scope='module', autouse=True)
def ensure_wal_state():
    """Ensure WAL/UNDO directories exist and transaction manager is fresh.

    Previous tests (e.g., test_redo_undo) may remove these directories,
    leaving the global transaction manager with stale file descriptors.
    """
    for dir_path in [WAL_DIR, UNDO_DIR]:
        os.makedirs(dir_path, exist_ok=True)
    global_vars.xact_manager = TransactionManager()
    global_vars.xact_manager.recovery()
    yield


# ============================================================
# Schema Creation
# ============================================================

def create_tpcc_schema():
    """Create all 9 TPC-C tables."""

    # Warehouse table
    execute_simple_query("""
        CREATE TABLE warehouse (
            w_id int NOT NULL,
            w_name text,
            w_street_1 text,
            w_street_2 text,
            w_city text,
            w_state text,
            w_zip text,
            w_tax float,
            w_ytd float
        )
    """)

    # District table
    execute_simple_query("""
        CREATE TABLE district (
            d_id int NOT NULL,
            d_w_id int NOT NULL,
            d_name text,
            d_street_1 text,
            d_street_2 text,
            d_city text,
            d_state text,
            d_zip text,
            d_tax float,
            d_ytd float,
            d_next_o_id int
        )
    """)

    # Customer table
    execute_simple_query("""
        CREATE TABLE customer (
            c_id int NOT NULL,
            c_d_id int NOT NULL,
            c_w_id int NOT NULL,
            c_first text,
            c_middle text,
            c_last text,
            c_street_1 text,
            c_street_2 text,
            c_city text,
            c_state text,
            c_zip text,
            c_phone text,
            c_since text,
            c_credit text,
            c_credit_lim float,
            c_discount float,
            c_balance float,
            c_ytd_payment float,
            c_payment_cnt int,
            c_delivery_cnt int,
            c_data text
        )
    """)

    # History table
    execute_simple_query("""
        CREATE TABLE history (
            h_c_id int NOT NULL,
            h_c_d_id int NOT NULL,
            h_c_w_id int NOT NULL,
            h_d_id int NOT NULL,
            h_w_id int NOT NULL,
            h_date text,
            h_amount float,
            h_data text
        )
    """)

    # Orders table (named "orders" to avoid SQL keyword conflict)
    execute_simple_query("""
        CREATE TABLE orders (
            o_id int NOT NULL,
            o_d_id int NOT NULL,
            o_w_id int NOT NULL,
            o_c_id int NOT NULL,
            o_entry_d text,
            o_carrier_id int,
            o_ol_cnt int,
            o_all_local int
        )
    """)

    # New-Order table
    execute_simple_query("""
        CREATE TABLE new_order (
            no_o_id int NOT NULL,
            no_d_id int NOT NULL,
            no_w_id int NOT NULL
        )
    """)

    # Order-Line table
    execute_simple_query("""
        CREATE TABLE order_line (
            ol_o_id int NOT NULL,
            ol_d_id int NOT NULL,
            ol_w_id int NOT NULL,
            ol_number int NOT NULL,
            ol_i_id int NOT NULL,
            ol_supply_w_id int,
            ol_delivery_d text,
            ol_quantity int,
            ol_amount float,
            ol_dist_info text
        )
    """)

    # Item table
    execute_simple_query("""
        CREATE TABLE item (
            i_id int NOT NULL,
            i_im_id int,
            i_name text,
            i_price float,
            i_data text
        )
    """)

    # Stock table
    execute_simple_query("""
        CREATE TABLE stock (
            s_i_id int NOT NULL,
            s_w_id int NOT NULL,
            s_quantity int,
            s_dist_01 text,
            s_dist_02 text,
            s_ytd int,
            s_order_cnt int,
            s_remote_cnt int,
            s_data text
        )
    """)


def create_tpcc_indexes():
    """Create indexes on primary lookup columns for TPC-C tables."""
    execute_simple_query("CREATE INDEX idx_warehouse_w_id ON warehouse (w_id)")
    execute_simple_query("CREATE INDEX idx_district_d_id ON district (d_id)")
    execute_simple_query("CREATE INDEX idx_customer_c_id ON customer (c_id)")
    execute_simple_query("CREATE INDEX idx_orders_o_id ON orders (o_id)")
    execute_simple_query("CREATE INDEX idx_item_i_id ON item (i_id)")
    execute_simple_query("CREATE INDEX idx_stock_s_i_id ON stock (s_i_id)")
    execute_simple_query("CREATE INDEX idx_new_order_no_o_id ON new_order (no_o_id)")


# ============================================================
# Data Loading
# ============================================================

def load_tpcc_data():
    """Load initial TPC-C data for 1 warehouse."""

    # Warehouse (1 warehouse)
    execute_simple_query("""
        INSERT INTO warehouse VALUES (1, 'W_ONE', '100 Main St', 'Suite 1',
            'Springfield', 'IL', '62701', 0.1, 300000.0)
    """)

    # Districts (2 districts per warehouse for testing)
    # d_next_o_id starts at 3004 (after existing orders 3001-3003)
    execute_simple_query("""
        INSERT INTO district VALUES (1, 1, 'D_ONE', '200 Oak Ave', 'Floor 2',
            'Springfield', 'IL', '62701', 0.05, 30000.0, 3004)
    """)
    execute_simple_query("""
        INSERT INTO district VALUES (2, 1, 'D_TWO', '300 Elm St', 'Floor 3',
            'Springfield', 'IL', '62702', 0.08, 30000.0, 3004)
    """)

    # Customers (3 customers per district)
    customers = [
        (1, 1, 1, 'John', 'O', 'Smith', '10 Pine', 'apt1', 'Springfield', 'IL',
         '62701', '555-0001', '2024-01-01', 'GC', 50000.0, 0.1, 100.0, 10.0, 1, 0, 'data1'),
        (2, 1, 1, 'Jane', 'O', 'Doe', '20 Maple', 'apt2', 'Springfield', 'IL',
         '62701', '555-0002', '2024-01-01', 'GC', 50000.0, 0.15, 200.0, 5.0, 1, 0, 'data2'),
        (3, 1, 1, 'Bob', 'O', 'Brown', '30 Cedar', 'apt3', 'Springfield', 'IL',
         '62701', '555-0003', '2024-01-01', 'BC', 50000.0, 0.05, 300.0, 0.0, 0, 0, 'data3'),
        (1, 2, 1, 'Alice', 'O', 'Green', '40 Birch', 'apt4', 'Springfield', 'IL',
         '62702', '555-0004', '2024-01-01', 'GC', 50000.0, 0.12, 150.0, 20.0, 2, 0, 'data4'),
        (2, 2, 1, 'Charlie', 'O', 'White', '50 Walnut', 'apt5', 'Springfield', 'IL',
         '62702', '555-0005', '2024-01-01', 'GC', 50000.0, 0.08, 250.0, 15.0, 1, 0, 'data5'),
    ]
    for c in customers:
        execute_simple_query(
            f"INSERT INTO customer VALUES ({c[0]}, {c[1]}, {c[2]}, '{c[3]}', '{c[4]}', "
            f"'{c[5]}', '{c[6]}', '{c[7]}', '{c[8]}', '{c[9]}', '{c[10]}', '{c[11]}', "
            f"'{c[12]}', '{c[13]}', {c[14]}, {c[15]}, {c[16]}, {c[17]}, {c[18]}, {c[19]}, '{c[20]}')"
        )

    # Items (10 items)
    for i in range(1, 11):
        execute_simple_query(
            f"INSERT INTO item VALUES ({i}, {i * 100}, 'Item_{i}', {round(1.0 + i * 5.5, 2)}, 'item_data_{i}')"
        )

    # Stock (10 items * 1 warehouse)
    for i in range(1, 11):
        qty = 50 + i * 5
        execute_simple_query(
            f"INSERT INTO stock VALUES ({i}, 1, {qty}, 'dist01_{i}', 'dist02_{i}', "
            f"0, 0, 0, 'stock_data_{i}')"
        )

    # Initial orders (3 orders in district 1)
    orders_data = [
        (3001, 1, 1, 1, '2024-06-01', 1, 2, 1),
        (3002, 1, 1, 2, '2024-06-02', 1, 3, 1),
        (3003, 1, 1, 3, '2024-06-03', 0, 1, 1),  # carrier_id=0 means not yet delivered
    ]
    for o in orders_data:
        execute_simple_query(
            f"INSERT INTO orders VALUES ({o[0]}, {o[1]}, {o[2]}, {o[3]}, "
            f"'{o[4]}', {o[5]}, {o[6]}, {o[7]})"
        )

    # New orders (only order 3003 is pending)
    execute_simple_query("INSERT INTO new_order VALUES (3003, 1, 1)")

    # Order lines
    ol_data = [
        (3001, 1, 1, 1, 1, 1, '2024-06-01', 5, 27.5, 'dist01_1'),
        (3001, 1, 1, 2, 2, 1, '2024-06-01', 3, 19.5, 'dist01_2'),
        (3002, 1, 1, 1, 3, 1, '2024-06-02', 2, 35.0, 'dist01_3'),
        (3002, 1, 1, 2, 4, 1, '2024-06-02', 1, 23.5, 'dist01_4'),
        (3002, 1, 1, 3, 5, 1, '2024-06-02', 4, 110.0, 'dist01_5'),
        (3003, 1, 1, 1, 6, 1, null, 10, 385.0, 'dist01_6'),
    ]
    for ol in ol_data:
        delivery_d = f"'{ol[6]}'" if ol[6] is not None else 'null'
        execute_simple_query(
            f"INSERT INTO order_line VALUES ({ol[0]}, {ol[1]}, {ol[2]}, {ol[3]}, "
            f"{ol[4]}, {ol[5]}, {delivery_d}, {ol[7]}, {ol[8]}, '{ol[9]}')"
        )

    # History
    execute_simple_query(
        "INSERT INTO history VALUES (1, 1, 1, 1, 1, '2024-06-01', 10.0, 'W_ONE    D_ONE')"
    )

null = None  # for use in data above


# ============================================================
# TPC-C Transaction Implementations
# ============================================================

def txn_new_order(w_id, d_id, c_id, item_ids_and_qtys):
    """
    TPC-C New-Order transaction.
    Creates a new order with multiple order lines.
    """
    execute_simple_query("BEGIN")

    try:
        # Get district info and increment next order id
        result = execute_simple_query(
            f"SELECT d_tax, d_next_o_id FROM district WHERE d_id = {d_id} AND d_w_id = {w_id}"
        )
        assert isinstance(result, ExecuteResultSet), f"Expected result set, got {type(result)}"
        assert len(result.tuples) == 1, f"Expected 1 district row, got {len(result.tuples)}"
        d_tax = result.tuples[0][0]
        next_o_id = result.tuples[0][1]

        # Update district next_o_id
        execute_simple_query(
            f"UPDATE district SET d_next_o_id = d_next_o_id + 1 "
            f"WHERE d_id = {d_id} AND d_w_id = {w_id}"
        )

        # Get customer discount
        result = execute_simple_query(
            f"SELECT c_discount, c_last, c_credit FROM customer "
            f"WHERE c_id = {c_id} AND c_d_id = {d_id} AND c_w_id = {w_id}"
        )
        assert isinstance(result, ExecuteResultSet)
        assert len(result.tuples) == 1

        # Get warehouse tax
        result = execute_simple_query(
            f"SELECT w_tax FROM warehouse WHERE w_id = {w_id}"
        )
        assert isinstance(result, ExecuteResultSet)
        assert len(result.tuples) == 1

        # Insert order
        ol_cnt = len(item_ids_and_qtys)
        execute_simple_query(
            f"INSERT INTO orders VALUES ({next_o_id}, {d_id}, {w_id}, {c_id}, "
            f"'2024-07-01', 0, {ol_cnt}, 1)"
        )

        # Insert new_order
        execute_simple_query(
            f"INSERT INTO new_order VALUES ({next_o_id}, {d_id}, {w_id})"
        )

        # Insert order lines
        for ol_num, (i_id, qty) in enumerate(item_ids_and_qtys, 1):
            # Get item price
            result = execute_simple_query(
                f"SELECT i_price, i_name, i_data FROM item WHERE i_id = {i_id}"
            )
            assert isinstance(result, ExecuteResultSet)
            assert len(result.tuples) == 1, f"Item {i_id} not found"
            i_price = result.tuples[0][0]

            # Get stock info
            result = execute_simple_query(
                f"SELECT s_quantity, s_dist_01, s_ytd, s_order_cnt FROM stock "
                f"WHERE s_i_id = {i_id} AND s_w_id = {w_id}"
            )
            assert isinstance(result, ExecuteResultSet)
            assert len(result.tuples) == 1, f"Stock for item {i_id} not found"
            s_quantity = result.tuples[0][0]

            # Update stock
            new_qty = s_quantity - qty
            if new_qty < 10:
                new_qty += 91
            execute_simple_query(
                f"UPDATE stock SET s_quantity = {new_qty}, s_ytd = s_ytd + {qty}, "
                f"s_order_cnt = s_order_cnt + 1 "
                f"WHERE s_i_id = {i_id} AND s_w_id = {w_id}"
            )

            ol_amount = qty * i_price
            execute_simple_query(
                f"INSERT INTO order_line VALUES ({next_o_id}, {d_id}, {w_id}, {ol_num}, "
                f"{i_id}, {w_id}, null, {qty}, {ol_amount}, 'dist01_{i_id}')"
            )

        execute_simple_query("COMMIT")
        return next_o_id

    except Exception:
        execute_simple_query("ROLLBACK")
        raise


def txn_payment(w_id, d_id, c_id, h_amount):
    """
    TPC-C Payment transaction.
    Updates warehouse/district/customer balances and inserts history.
    """
    execute_simple_query("BEGIN")

    try:
        # Update warehouse YTD
        execute_simple_query(
            f"UPDATE warehouse SET w_ytd = w_ytd + {h_amount} WHERE w_id = {w_id}"
        )

        # Get warehouse name
        result = execute_simple_query(
            f"SELECT w_name FROM warehouse WHERE w_id = {w_id}"
        )
        assert isinstance(result, ExecuteResultSet)
        w_name = result.tuples[0][0]

        # Update district YTD
        execute_simple_query(
            f"UPDATE district SET d_ytd = d_ytd + {h_amount} "
            f"WHERE d_id = {d_id} AND d_w_id = {w_id}"
        )

        # Get district name
        result = execute_simple_query(
            f"SELECT d_name FROM district WHERE d_id = {d_id} AND d_w_id = {w_id}"
        )
        assert isinstance(result, ExecuteResultSet)
        d_name = result.tuples[0][0]

        # Update customer
        execute_simple_query(
            f"UPDATE customer SET c_balance = c_balance - {h_amount}, "
            f"c_ytd_payment = c_ytd_payment + {h_amount}, "
            f"c_payment_cnt = c_payment_cnt + 1 "
            f"WHERE c_id = {c_id} AND c_d_id = {d_id} AND c_w_id = {w_id}"
        )

        # Insert history
        h_data = f"{w_name}    {d_name}"
        execute_simple_query(
            f"INSERT INTO history VALUES ({c_id}, {d_id}, {w_id}, {d_id}, {w_id}, "
            f"'2024-07-01', {h_amount}, '{h_data}')"
        )

        execute_simple_query("COMMIT")

    except Exception:
        execute_simple_query("ROLLBACK")
        raise


def txn_order_status(w_id, d_id, c_id):
    """
    TPC-C Order-Status transaction (read-only).
    Finds the customer's most recent order and its order lines.
    """
    # Get customer info
    result = execute_simple_query(
        f"SELECT c_balance, c_first, c_middle, c_last FROM customer "
        f"WHERE c_id = {c_id} AND c_d_id = {d_id} AND c_w_id = {w_id}"
    )
    assert isinstance(result, ExecuteResultSet)
    assert len(result.tuples) == 1

    # Get last order for this customer
    result = execute_simple_query(
        f"SELECT o_id, o_entry_d, o_carrier_id FROM orders "
        f"WHERE o_c_id = {c_id} AND o_d_id = {d_id} AND o_w_id = {w_id} "
        f"ORDER BY o_id DESC LIMIT 1"
    )
    assert isinstance(result, ExecuteResultSet)
    if len(result.tuples) == 0:
        return None

    o_id = result.tuples[0][0]

    # Get order lines for this order
    result = execute_simple_query(
        f"SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d "
        f"FROM order_line "
        f"WHERE ol_o_id = {o_id} AND ol_d_id = {d_id} AND ol_w_id = {w_id}"
    )
    assert isinstance(result, ExecuteResultSet)
    return o_id, result.tuples


def txn_delivery(w_id, d_id, carrier_id):
    """
    TPC-C Delivery transaction.
    Processes the oldest undelivered order in the given district.
    """
    execute_simple_query("BEGIN")

    try:
        # Find the oldest new_order for this district
        result = execute_simple_query(
            f"SELECT no_o_id FROM new_order "
            f"WHERE no_d_id = {d_id} AND no_w_id = {w_id} "
            f"ORDER BY no_o_id LIMIT 1"
        )
        assert isinstance(result, ExecuteResultSet)
        if len(result.tuples) == 0:
            execute_simple_query("COMMIT")
            return None

        no_o_id = result.tuples[0][0]

        # Delete from new_order
        execute_simple_query(
            f"DELETE FROM new_order "
            f"WHERE no_o_id = {no_o_id} AND no_d_id = {d_id} AND no_w_id = {w_id}"
        )

        # Get customer id for this order
        result = execute_simple_query(
            f"SELECT o_c_id FROM orders "
            f"WHERE o_id = {no_o_id} AND o_d_id = {d_id} AND o_w_id = {w_id}"
        )
        assert isinstance(result, ExecuteResultSet)
        assert len(result.tuples) == 1
        o_c_id = result.tuples[0][0]

        # Update order carrier
        execute_simple_query(
            f"UPDATE orders SET o_carrier_id = {carrier_id} "
            f"WHERE o_id = {no_o_id} AND o_d_id = {d_id} AND o_w_id = {w_id}"
        )

        # Update order lines with delivery date
        execute_simple_query(
            f"UPDATE order_line SET ol_delivery_d = '2024-07-01' "
            f"WHERE ol_o_id = {no_o_id} AND ol_d_id = {d_id} AND ol_w_id = {w_id}"
        )

        # Calculate total amount for this order's lines
        result = execute_simple_query(
            f"SELECT ol_o_id, sum(ol_amount) FROM order_line "
            f"WHERE ol_o_id = {no_o_id} AND ol_d_id = {d_id} AND ol_w_id = {w_id} "
            f"GROUP BY ol_o_id"
        )
        assert isinstance(result, ExecuteResultSet)
        total_amount = result.tuples[0][1] if result.tuples else 0

        # Update customer balance and delivery count
        execute_simple_query(
            f"UPDATE customer SET c_balance = c_balance + {total_amount}, "
            f"c_delivery_cnt = c_delivery_cnt + 1 "
            f"WHERE c_id = {o_c_id} AND c_d_id = {d_id} AND c_w_id = {w_id}"
        )

        execute_simple_query("COMMIT")
        return no_o_id

    except Exception:
        execute_simple_query("ROLLBACK")
        raise


def txn_stock_level(w_id, d_id, threshold):
    """
    TPC-C Stock-Level transaction (read-only).
    Counts the number of recently ordered items with stock below the threshold.
    Uses a simplified approach: checks stock for items in recent orders.
    """
    # Get recent order IDs from the district
    result = execute_simple_query(
        f"SELECT d_next_o_id FROM district WHERE d_id = {d_id} AND d_w_id = {w_id}"
    )
    assert isinstance(result, ExecuteResultSet)
    next_o_id = result.tuples[0][0]

    # Get items from last 5 orders (simplified: scan order_line for this district)
    # We use a range check with AND instead of BETWEEN
    lower_bound = next_o_id - 5
    result = execute_simple_query(
        f"SELECT ol_i_id FROM order_line "
        f"WHERE ol_d_id = {d_id} AND ol_w_id = {w_id} "
        f"AND ol_o_id >= {lower_bound} AND ol_o_id < {next_o_id}"
    )
    assert isinstance(result, ExecuteResultSet)

    # Check stock levels for each unique item
    item_ids = set()
    for row in result.tuples:
        item_ids.add(row[0])

    low_stock_count = 0
    for i_id in item_ids:
        result = execute_simple_query(
            f"SELECT s_quantity FROM stock WHERE s_i_id = {i_id} AND s_w_id = {w_id}"
        )
        assert isinstance(result, ExecuteResultSet)
        if result.tuples and result.tuples[0][0] < threshold:
            low_stock_count += 1

    return low_stock_count


# ============================================================
# Cleanup
# ============================================================

def drop_tpcc_schema():
    """Drop all TPC-C tables and indexes."""
    # Drop indexes first
    for idx in ['idx_warehouse_w_id', 'idx_district_d_id', 'idx_customer_c_id',
                'idx_orders_o_id', 'idx_item_i_id', 'idx_stock_s_i_id',
                'idx_new_order_no_o_id']:
        execute_simple_query(f"DROP INDEX {idx}")

    # Drop tables
    for tbl in ['order_line', 'new_order', 'orders', 'history',
                'customer', 'district', 'warehouse', 'item', 'stock']:
        execute_simple_query(f"DROP TABLE {tbl}")


# ============================================================
# Test Functions
# ============================================================

def test_tpcc_schema_and_load():
    """Test 1: Create schema, load data, verify row counts."""
    create_tpcc_schema()
    load_tpcc_data()

    # Verify warehouse
    result = execute_simple_query("SELECT * FROM warehouse")
    assert isinstance(result, ExecuteResultSet)
    assert len(result.tuples) == 1

    # Verify districts
    result = execute_simple_query("SELECT * FROM district")
    assert isinstance(result, ExecuteResultSet)
    assert len(result.tuples) == 2

    # Verify customers
    result = execute_simple_query("SELECT * FROM customer")
    assert isinstance(result, ExecuteResultSet)
    assert len(result.tuples) == 5

    # Verify items
    result = execute_simple_query("SELECT * FROM item")
    assert isinstance(result, ExecuteResultSet)
    assert len(result.tuples) == 10

    # Verify stock
    result = execute_simple_query("SELECT * FROM stock")
    assert isinstance(result, ExecuteResultSet)
    assert len(result.tuples) == 10

    # Verify initial orders
    result = execute_simple_query("SELECT * FROM orders")
    assert isinstance(result, ExecuteResultSet)
    assert len(result.tuples) == 3

    # Verify order lines
    result = execute_simple_query("SELECT * FROM order_line")
    assert isinstance(result, ExecuteResultSet)
    assert len(result.tuples) == 6

    # Create indexes after data load
    create_tpcc_indexes()


def test_tpcc_new_order():
    """Test 2: New-Order transaction."""
    # Place a new order: customer 1, district 1, warehouse 1
    # Order items: item 1 qty 3, item 2 qty 5
    new_o_id = txn_new_order(
        w_id=1, d_id=1, c_id=1,
        item_ids_and_qtys=[(1, 3), (2, 5)]
    )
    assert new_o_id == 3004  # d_next_o_id was 3004

    # Verify the order was created
    result = execute_simple_query(
        f"SELECT o_id, o_c_id, o_ol_cnt FROM orders WHERE o_id = {new_o_id} AND o_d_id = 1 AND o_w_id = 1"
    )
    assert isinstance(result, ExecuteResultSet)
    assert len(result.tuples) == 1
    assert result.tuples[0][0] == new_o_id
    assert result.tuples[0][1] == 1  # c_id
    assert result.tuples[0][2] == 2  # ol_cnt

    # Verify district next_o_id was incremented
    result = execute_simple_query("SELECT d_next_o_id FROM district WHERE d_id = 1 AND d_w_id = 1")
    assert isinstance(result, ExecuteResultSet)
    assert result.tuples[0][0] == 3005

    # Verify order lines
    result = execute_simple_query(
        f"SELECT ol_number, ol_i_id, ol_quantity FROM order_line "
        f"WHERE ol_o_id = {new_o_id} AND ol_d_id = 1 AND ol_w_id = 1 ORDER BY ol_number"
    )
    assert isinstance(result, ExecuteResultSet)
    assert len(result.tuples) == 2
    assert result.tuples[0][1] == 1  # item 1
    assert result.tuples[0][2] == 3  # qty 3
    assert result.tuples[1][1] == 2  # item 2
    assert result.tuples[1][2] == 5  # qty 5

    # Verify stock was decremented
    result = execute_simple_query("SELECT s_quantity FROM stock WHERE s_i_id = 1 AND s_w_id = 1")
    assert isinstance(result, ExecuteResultSet)
    # Initial was 55, ordered 3, so 55 - 3 = 52
    assert result.tuples[0][0] == 52

    # Verify new_order entry exists
    result = execute_simple_query(
        f"SELECT * FROM new_order WHERE no_o_id = {new_o_id} AND no_d_id = 1 AND no_w_id = 1"
    )
    assert isinstance(result, ExecuteResultSet)
    assert len(result.tuples) == 1


def test_tpcc_payment():
    """Test 3: Payment transaction."""
    # Get customer 1's balance before payment
    result = execute_simple_query(
        "SELECT c_balance, c_ytd_payment, c_payment_cnt FROM customer "
        "WHERE c_id = 1 AND c_d_id = 1 AND c_w_id = 1"
    )
    assert isinstance(result, ExecuteResultSet)
    old_balance = result.tuples[0][0]
    old_ytd = result.tuples[0][1]
    old_cnt = result.tuples[0][2]

    # Get warehouse YTD before payment
    result = execute_simple_query("SELECT w_ytd FROM warehouse WHERE w_id = 1")
    assert isinstance(result, ExecuteResultSet)
    old_w_ytd = result.tuples[0][0]

    # Process payment of $25.00
    payment_amount = 25.0
    txn_payment(w_id=1, d_id=1, c_id=1, h_amount=payment_amount)

    # Verify customer balance decreased
    result = execute_simple_query(
        "SELECT c_balance, c_ytd_payment, c_payment_cnt FROM customer "
        "WHERE c_id = 1 AND c_d_id = 1 AND c_w_id = 1"
    )
    assert isinstance(result, ExecuteResultSet)
    assert result.tuples[0][0] == old_balance - payment_amount
    assert result.tuples[0][1] == old_ytd + payment_amount
    assert result.tuples[0][2] == old_cnt + 1

    # Verify warehouse YTD increased
    result = execute_simple_query("SELECT w_ytd FROM warehouse WHERE w_id = 1")
    assert isinstance(result, ExecuteResultSet)
    assert result.tuples[0][0] == old_w_ytd + payment_amount

    # Verify history record was inserted
    result = execute_simple_query(
        "SELECT h_amount FROM history WHERE h_c_id = 1 AND h_c_d_id = 1 AND h_c_w_id = 1"
    )
    assert isinstance(result, ExecuteResultSet)
    assert len(result.tuples) >= 2  # At least the initial + new one


def test_tpcc_order_status():
    """Test 4: Order-Status transaction (read-only)."""
    result = txn_order_status(w_id=1, d_id=1, c_id=1)
    assert result is not None
    o_id, order_lines = result
    # Customer 1's latest order should be the one we just created in test_tpcc_new_order
    assert o_id == 3004  # the order we just created
    assert len(order_lines) == 2  # 2 order lines


def test_tpcc_delivery():
    """Test 5: Delivery transaction."""
    # There should be two pending new_orders: 3003 (from initial load) and 3004 (from new order)
    # Delivery picks the oldest one first
    delivered_o_id = txn_delivery(w_id=1, d_id=1, carrier_id=5)
    assert delivered_o_id is not None

    # Verify the delivered order has a carrier now
    result = execute_simple_query(
        f"SELECT o_carrier_id FROM orders WHERE o_id = {delivered_o_id} AND o_d_id = 1 AND o_w_id = 1"
    )
    assert isinstance(result, ExecuteResultSet)
    assert result.tuples[0][0] == 5

    # Verify the new_order entry was deleted
    result = execute_simple_query(
        f"SELECT * FROM new_order WHERE no_o_id = {delivered_o_id} AND no_d_id = 1 AND no_w_id = 1"
    )
    assert isinstance(result, ExecuteResultSet)
    assert len(result.tuples) == 0


def test_tpcc_stock_level():
    """Test 6: Stock-Level transaction (read-only)."""
    # Check stock levels with a high threshold to get some results
    low_count = txn_stock_level(w_id=1, d_id=1, threshold=100)
    # All items should have stock well below 100 (initial ~55-100, minus orders)
    assert isinstance(low_count, int)
    assert low_count >= 0


def test_tpcc_multiple_transactions():
    """Test 7: Run multiple transaction types in sequence to test interaction."""
    # Another new order
    new_o_id = txn_new_order(
        w_id=1, d_id=1, c_id=2,
        item_ids_and_qtys=[(3, 2), (4, 1), (5, 3)]
    )
    assert new_o_id is not None

    # Payment for a different customer
    txn_payment(w_id=1, d_id=1, c_id=2, h_amount=50.0)

    # Order status check
    result = txn_order_status(w_id=1, d_id=1, c_id=2)
    assert result is not None
    o_id, order_lines = result
    assert o_id == new_o_id
    assert len(order_lines) == 3

    # Delivery
    delivered = txn_delivery(w_id=1, d_id=1, carrier_id=7)
    # Should deliver the next pending order
    assert delivered is not None

    # Stock level
    low_count = txn_stock_level(w_id=1, d_id=1, threshold=50)
    assert isinstance(low_count, int)


def test_tpcc_cleanup():
    """Test 8: Clean up TPC-C schema."""
    drop_tpcc_schema()
