import sqlite3
import csv

class EcommerceApp:
    def __init__(self):
        self.connection = sqlite3.connect("ecommerce.db")
        self.cursor = self.connection.cursor()
        self.create_tables()

    def create_tables(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                product_id TEXT PRIMARY KEY,
                product_name TEXT NOT NULL,
                product_price REAL NOT NULL
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS product_transactions (
                transaction_id INTEGER PRIMARY KEY,
                product_id TEXT NOT NULL,
                price REAL NOT NULL,
                quantity INTEGER NOT NULL,
                type TEXT NOT NULL,
                FOREIGN KEY (product_id) REFERENCES products (product_id)
            )
        ''')
        self.connection.commit()

    def save_product(self, product_id, product_name, product_price):
        self.cursor.execute('''
            INSERT OR REPLACE INTO products (product_id, product_name, product_price)
            VALUES (?, ?, ?)
        ''', (product_id, product_name, product_price))
        self.connection.commit()

    def purchase_product(self, product_id, quantity, price):
        self.cursor.execute('''
            INSERT INTO product_transactions (product_id, price, quantity, type)
            VALUES (:product_id, :price, :quantity, 'purchase')
        ''', {'product_id': product_id, 'price': price, 'quantity': quantity})
        self.connection.commit()

    def order_product(self, product_id, quantity):
        self.cursor.execute('''
            INSERT INTO product_transactions (product_id, price, quantity, type)
            VALUES (:product_id, (
                SELECT product_price
                FROM products
                WHERE product_id = :product_id
            ), :quantity, 'order')
        ''', {'product_id': product_id, 'quantity': quantity})
        self.connection.commit()

    def get_quantity_of_product(self, product_id):
        self.cursor.execute('''
            SELECT SUM(quantity)
            FROM product_transactions
            WHERE product_id = :product_id AND type = 'purchase'
        ''', {'product_id': product_id})
        purchased = self.cursor.fetchone()[0] or 0

        self.cursor.execute('''
            SELECT SUM(quantity)
            FROM product_transactions
            WHERE product_id = :product_id AND type = 'order'
        ''', {'product_id': product_id})
        ordered = self.cursor.fetchone()[0] or 0

        return purchased - ordered

    def get_average_price(self, product_id):
        self.cursor.execute('''
            SELECT SUM(price * quantity) / SUM(quantity)
            FROM product_transactions
            WHERE product_id = :product_id AND type = 'purchase'
        ''', {'product_id': product_id})
        result = self.cursor.fetchone()
        return result[0] if result is not None else None

    def get_product_profit(self, product_id):
        self.cursor.execute('''
            SELECT
                SUM(CASE WHEN type = 'purchase' THEN price * quantity ELSE 0 END) as total_purchase_cost,
                SUM(CASE WHEN type = 'purchase' THEN quantity ELSE 0 END) as total_purchase_quantity,
                SUM(CASE WHEN type = 'order' THEN price * quantity ELSE 0 END) as total_order_revenue,
                SUM(CASE WHEN type = 'order' THEN quantity ELSE 0 END) as total_order_quantity
            FROM product_transactions
            WHERE product_id = :product_id
        ''', {'product_id': product_id})
        result = self.cursor.fetchone()
        if result:
            total_purchase_cost, total_purchase_quantity, total_order_revenue, total_order_quantity = result
            average_purchase_price = total_purchase_cost / total_purchase_quantity
            average_order_price = total_order_revenue / total_order_quantity
            profit_per_unit = average_order_price - average_purchase_price
            total_profit = profit_per_unit * total_order_quantity
            return total_profit
        return 0
    

    def get_fewest_product(self):
        self.cursor.execute('''
            SELECT
                p.product_name,
                SUM(CASE WHEN pt.type = 'purchase' THEN pt.quantity ELSE -pt.quantity END) as remaining_quantity
            FROM products p
            JOIN product_transactions pt ON p.product_id = pt.product_id
            GROUP BY p.product_id, p.product_name
            HAVING remaining_quantity >= 0
            ORDER BY remaining_quantity ASC, p.product_name ASC
            LIMIT 1
        ''')
        result = self.cursor.fetchone()
        if result:
            return result[0]
        return None

    def get_most_popular_product(self):
        self.cursor.execute('''
            SELECT
                p.product_name,
                COUNT(pt.type) as order_count
            FROM products p
            JOIN product_transactions pt ON p.product_id = pt.product_id
            WHERE pt.type = 'order'
            GROUP BY p.product_id, p.product_name
            ORDER BY order_count DESC, p.product_name ASC
            LIMIT 1
        ''')
        result = self.cursor.fetchone()
        if result:
            return result[0]
        return None
    

    def get_orders_report(self):
        self.cursor.execute('''
            SELECT
                pt.product_id,
                p.product_name,
                pt.quantity,
                pt.price,
                pt.quantity * (SELECT AVG(price) FROM product_transactions WHERE product_id = pt.product_id AND type = 'purchase') as COGS,
                pt.quantity * pt.price as selling_price
            FROM product_transactions pt
            JOIN products p ON p.product_id = pt.product_id
            WHERE pt.type = 'order'
            ORDER BY pt.date ASC
        ''')
        report = self.cursor.fetchall()
        return report
    
    def export_orders_report(self, path):
        report = self.get_orders_report()
        with open(path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Product ID', 'Product Name', 'Quantity', 'Price', 'COGS', 'Selling Price'])
            writer.writerows(report)

    def run(self):
        while True:
            command = input("> ")
            command_parts = command.strip().split()

            try:
                if command_parts[0] == "save_product":
                    self.save_product(command_parts[1], command_parts[2], float(command_parts[3]))
                elif command_parts[0] == "purchase_product":
                    self.purchase_product(command_parts[1], int(command_parts[2]), float(command_parts[3]))
                elif command_parts[0] == "order_product":
                    self.order_product(command_parts[1], int(command_parts[2]))
                elif command_parts[0] == "get_quantity_of_product":
                    quantity = self.get_quantity_of_product(command_parts[1])
                    print(quantity)
                elif command_parts[0] == "get_average_price":
                    avg_price = self.get_average_price(command_parts[1])
                    print(avg_price)
                elif command_parts[0] == "get_product_profit":
                    profit = self.get_product_profit(command_parts[1])
                    print(profit)
                elif command_parts[0] == "get_fewest_product":
                    fewest_product = self.get_fewest_product()
                    print(fewest_product)
                elif command_parts[0] == "get_most_popular_product":
                    most_popular_product = self.get_most_popular_product()
                    print(most_popular_product)
                elif command_parts[0] == "exit":
                    self.connection.close()
                    break
                elif command[0] == "get_orders_report":
                    report = self.get_orders_report()
                    print("Product ID | Product Name | Quantity | Price | COGS | Selling Price")
                    for row in report:
                        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]}")
                elif command[0] == "export_orders_report":
                    self.export_orders_report(command[1])
            except Exception as e:
                print(f"Error: {e}")

if __name__ == "__main__":
    app = EcommerceApp()
    app.run()
