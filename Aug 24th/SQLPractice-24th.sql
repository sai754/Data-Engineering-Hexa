

CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Email VARCHAR(100),
    PhoneNumber VARCHAR(15)
);

INSERT INTO Customers (FirstName, LastName, Email, PhoneNumber)
VALUES 
('amit', 'sharma', 'amit.sharma@example.com', '9876543210'),
('priya', 'mehta', 'priya.mehta@example.com', '8765432109'),
('rohit', 'kumar', 'rohit.kumar@example.com', '7654321098'),
('neha', 'verma', 'neha.verma@example.com', '6543210987'),
('siddharth', 'singh', 'siddharth.singh@example.com', '5432109876'),
('asha', 'rao', 'asha.rao@example.com', '4321098765');


-- Data cleansing
-- Like trimming spaces etc

UPDATE Customers
SET FirstName = LTRIM(RTRIM(LOWER(FirstName))),
	LastName = LTRIM(RTRIM(LOWER(LastName)));

-- Regex

SELECT * FROM Customers WHERE FirstName LIKE 'A%';

SELECT * FROM Customers WHERE PhoneNumber
LIKE '[0-9][0-9][0-9]-[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'

-- Returns Lastname with 5 characters
SELECT * FROM Customers WHERE LastName LIKE '_____'

-- Partition By

SELECT CustomerID, OrderID, TotalAmount,
SUM(TotalAmount) OVER (PARTITION BY CustomerID ORDER BY OrderDate) 
as RunningTotal FROM Orders 

-- Ranking

SELECT CustomerID, TotalSales,
	RANK() OVER (ORDER BY TotalSales DESC) AS SalesRank
FROM (
	SELECT CustomerID, SUM(TotalAmount) AS TotalSales
	from Orders
	GROUP BY CustomerID
) AS SalesData

-- CTE Common Table Expression

CREATE TABLE Employees1 (

    EmployeeID INT PRIMARY KEY IDENTITY(1,1),

    EmployeeName VARCHAR(100),

    ManagerID INT NULL

);

 
INSERT INTO Employees1 (EmployeeName, ManagerID)

VALUES 

('Amit Sharma', NULL),  -- Top manager

('Priya Mehta', 1),     -- Reports to Amit

('Rohit Kumar', 1),     -- Reports to Amit

('Neha Verma', 2),      -- Reports to Priya

('Siddharth Singh', 2), -- Reports to Priya

('Asha Rao', 3);        -- Reports to Rohit

INSERT INTO Employees1 (EmployeeName, ManagerID)
VALUES 
('Vikram Gupta', 4),  -- Reports to Neha
('Rajesh Patel', 5);  -- Reports to Siddharth
 

WITH RecursiveEmployeeCTE AS (
	SELECT EmployeeID, ManagerID, EmployeeName
	FROM Employees1
	WHERE ManagerID IS NULL
	UNION ALL
	SELECT e.EmployeeID, e.ManagerID, e.EmployeeName
	FROM Employees1 e
	INNER JOIN RecursiveEmployeeCTE r on e.ManagerID = r.EmployeeID
)

SELECT * FROM RecursiveEmployeeCTE

-- RollUp Aggregate

CREATE TABLE Sales (

    SaleID INT PRIMARY KEY,

    ProductID INT,

    Category VARCHAR(50),

    Amount DECIMAL(10, 2),

    SaleDate DATE

);
 
INSERT INTO Sales (SaleID, ProductID, Category, Amount, SaleDate) 

VALUES 

(1, 101, 'Electronics', 1500.00, '2024-01-15'),

(2, 102, 'Furniture', 800.00, '2024-01-16'),

(3, 103, 'Electronics', 2000.00, '2024-01-17'),

(4, 104, 'Electronics', 1200.00, '2024-02-01'),

(5, 105, 'Furniture', 900.00, '2024-02-03');

select * from Sales

SELECT Category, SUM(Amount) AS TotalSales
FROM Sales
Group by Rollup(Category)
 
-- Corelated Subquery

CREATE TABLE Orders2 (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderAmount DECIMAL(10, 2),
    OrderDate DATE
);
 
INSERT INTO Orders2 (OrderID, CustomerID, OrderAmount, OrderDate)
VALUES 
(1, 1, 500.00, '2024-01-15'),
(2, 2, 700.00, '2024-01-16'),
(3, 1, 300.00, '2024-01-17'),
(4, 3, 1200.00, '2024-02-01'),
(5, 2, 900.00, '2024-02-03');

SELECT DISTINCT o1.CustomerID
FROM Orders2 o1
WHERE ( SELECT COUNT(*) FROM Orders2 o2 
		WHERE o2.CustomerID = o1.CustomerID
) > 1;

-- Views
-- View is a subset of the main table
-- With the help of view you can get a subset of most important columns
-- Instead of having to get all columns
-- With schemabinding the view will be binded with the main parent table
-- View can be modified but it will not affect the main parent
-- But when parent is modified view has to be modified

CREATE TABLE ProductSales (
    SaleID INT PRIMARY KEY,
    ProductID INT,
    Amount DECIMAL(10, 2),
    SaleDate DATE
);
 
INSERT INTO ProductSales (SaleID, ProductID, Amount, SaleDate)
VALUES 
(1, 101, 1500.00, '2024-01-15'),
(2, 102, 800.00, '2024-01-16'),
(3, 103, 2000.00, '2024-01-17'),
(4, 104, 1200.00, '2024-02-01'),
(5, 105, 900.00, '2024-02-03');

CREATE VIEW TotalSalesByProduct
WITH SCHEMABINDING
AS
SELECT ProductID, SUM(Amount) AS TotalSales
from dbo.ProductSales
GROUP BY ProductID

SELECT * FROM TotalSalesByProduct

-- Data Warehouse life cycle

-- 1. Requirements Gathering
-- 2. Design Schema -- SnowFlake schema, Choose ETL Process
-- 3. Dev - Build ETL Pipelines, warehouse schema
-- 4. Testing
-- 5. Deployment - warehouse to production
-- 6. Maintenance

-- Fact Table - Stores Quantitative data, stores numerical values

-- Dimension Table - Stores descriptive data that provides context to the
-- facts. Textual attributes contains descriptive fields

-- Star Schema - Fact table is in the centre and dimension tables are 
-- around it