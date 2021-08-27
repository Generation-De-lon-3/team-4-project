CREATE DATABASE cafe;

\c cafe;

CREATE TABLE IF NOT EXISTS products (
	product_id SERIAL NOT NULL,
	product_name varchar(100) NOT NULL,
	product_size varchar(10),
	product_price float NOT NULL,
	UNIQUE (product_name, product_size, product_price),
PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS branches (
	branch_id SERIAL NOT NULL,
	branch_name varchar(22) NOT NULL,
<<<<<<< HEAD
=======
	UNIQUE (branch_name),
>>>>>>> stefan
PRIMARY KEY (branch_id)
);

CREATE TABLE IF NOT EXISTS orders (
	order_id SERIAL NOT NULL,
	order_timestamp timestamp NOT NULL,
	branch_id int NOT NULL,
	FOREIGN KEY (branch_id) REFERENCES branches (branch_id),
<<<<<<< HEAD
PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS payments (
	payment_id int NOT NULL,
	payment_method varchar(10) NOT NULL,
	card_type varchar(22),
	total_payment float NOT NULL,
	order_id int NOT NULL,
	FOREIGN KEY (order_id) REFERENCES orders (order_id),
PRIMARY KEY (payment_id)
);

CREATE TABLE IF NOT EXISTS baskets (
	basket_id SERIAL NOT NULL,
=======
	UNIQUE (order_timestamp, branch_id),
PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS baskets (
>>>>>>> stefan
	order_id int NOT NULL,
	product_id int NOT NULL,
	product_quantity int NOT NULL,
	FOREIGN KEY (product_id) REFERENCES products (product_id),
	FOREIGN KEY (order_id) REFERENCES orders (order_id),
	UNIQUE (order_id, product_id),
PRIMARY KEY (order_id, product_id)
);

CREATE TABLE IF NOT EXISTS payments (
	payment_id int NOT NULL,
	payment_method varchar(10) NOT NULL,
	card_type varchar(22),
	payment_total float NOT NULL,
	FOREIGN KEY (payment_id) REFERENCES orders (order_id),
	UNIQUE (payment_id),
PRIMARY KEY (payment_id)
);

