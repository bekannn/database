CREATE TABLE Customers (
    customer_id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    phone VARCHAR(20) NOT NULL,
    address VARCHAR(100) NOT NULL
);
--CREATE TYPE user_role AS ENUM ('Admin', 'Investor');


CREATE TYPE product_choice AS ENUM ('Sleeveless', 'T-shirt', 'Long-sleeve', 'Trouser', 'Shorts', 'Skirt');
CREATE TABLE Products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    category product_choice NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    quantity INT NOT NULL
);


CREATE TABLE Orders (
    order_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES Products(product_id),
    customer_id INT REFERENCES Customers(customer_id),
    order_date DATE NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    UNIQUE (order_id, product_id)

);

CREATE TABLE Financial_Transactions (
    transaction_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    amount DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);



/*CREATE TABLE Users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    phone INT(10) NOT NULL,
    address VARCHAR(100) NOT NULL
);
CREATE TYPE user_role AS ENUM ('Admin', 'Investor');


CREATE TABLE Portfolios (
    portfoli_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES Users(user_id),
    portfoli_name VARCHAR(100) NOT NULL
);

CREATE TYPE investment_type_choice AS ENUM ('Stock', 'Bond', 'ETF', 'Mutual Fund', 'Crypto');
--create ENUM type to specify the investment type

CREATE TABLE Investments (
    investment_id SERIAL PRIMARY KEY,
    portfoli_id INT REFERENCES Portfolios(portfoli_id),
    ticker VARCHAR(10) NOT NULL,
    investment_type investment_type_choice NOT NULL,
    purchase_date DATE NOT NULL,
    purchase_price DECIMAL(10, 2) NOT NULL,
    quantity INT NOT NULL,
    current_price DECIMAL(10, 2),
)

CREATE TYPE transaction_choice AS ENUM ('Buy', 'Sell');

CREATE TABLE Transactions (
    transaction_id SERIAL PRIMARY Key,
    investment_id INT REFERENCES Investments(investment_id),
    transaction_date DATE NOT NULL,
    transaction_type transaction_choice NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    quantity INT NOT NULL
); */


/*CREATE TABLE Investments (
    investment_id SERIAL PRIMARY KEY,
    investor_id INT REFERENCES Investors(investor_id),
    investment_type investment_type_choice NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    investment_date DATE NOT NULL
);*/