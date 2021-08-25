CREATE DATABASE cafe;

\c cafe;

CREATE TABLE IF NOT EXISTS products (
	product_id SERIAL NOT NULL,
	product_name varchar(100) NOT NULL,
	product_size varchar(10),
	product_price float NOT NULL,
PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS branch (
	branch_id SERIAL NOT NULL,
	branch varchar(22) NOT NULL,
PRIMARY KEY (branch_id)
);

CREATE TABLE IF NOT EXISTS payments (
	payment_id int NOT NULL,
	payment_method varchar(10) NOT NULL,
	card_type varchar(10),
PRIMARY KEY (payment_id)
);

CREATE TABLE IF NOT EXISTS orders (
	order_id SERIAL NOT NULL,
	timestamp_of_purchase timestamp NOT NULL,
	branch_id int NOT NULL,
	total_price float NOT NULL,
	payment_id int NOT NULL,
	FOREIGN KEY (branch_id) REFERENCES branch (branch_id),
	FOREIGN KEY (payment_id) REFERENCES payments (payment_id),
PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS basket (
	basket_id SERIAL NOT NULL,
	order_id int NOT NULL,
	product_id int NOT NULL,
	product_quantity int NOT NULL,
	FOREIGN KEY (product_id) REFERENCES products (product_id),
	FOREIGN KEY (order_id) REFERENCES orders (order_id),
PRIMARY KEY (basket_id)
);