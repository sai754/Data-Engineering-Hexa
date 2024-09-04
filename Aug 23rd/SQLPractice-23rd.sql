
use company

-- Math Functions

select product_name, Price, Round(Price, 2) 
as RoundedPrice from Products

select product_name, price, CEILING(price) as CeilingPrice
from Products

select product_name, price, FLOOR(price) as FloorPrice 
from Products

select product_name, price, SQRT(price) as SquareRootPrice
from Products

select product_name, price, POWER(price,2) as PriceSquared
from Products

select product_name, price, price % 5 as ModuloPrice 
from Products

select ABS(MAX(price) - MIN(price)) as PriceDifference
from Products

select product_name, price, ROUND(RAND() * 100,2) as RandomDiscountPercentage
from Products

select product_name, price, LOG(price) as LogarithmPrice
from Products

select product_name, price, 
(price * 0.85) as DiscountedPrice,
CEILING(price * 0.85) as CeilingDiscount, 
FLOOR(price * 0.85) as FloorDiscount
from Products

-- Aggregate Functions

select sum(price) as TotalSales from Products

select avg(price) as AveragePrice from Products

select count(product_name) as TotalProducts from Products

select MIN(price) as Minprice, MAX(price) as MaxPrice 
from Products

select category, count(product_id) as ProductCount from Products
group by category

-- Stored Procedure

-- The data will be compiled and stored in the cache and will be faster
-- It is reusable
-- We can combine multiple sql statements into one procedure
-- It is secure because we will be only using the procedure 

CREATE PROCEDURE GetAllProducts
AS
BEGIN
	SELECT * FROM Products;
END

exec GetAllProducts

-- Stored Proc which accepts the parameter
CREATE PROCEDURE GetProductByID
	@ProductID INT
AS
BEGIN
	SELECT * FROM Products
	WHERE product_id = @ProductID
END;

exec GetProductByID @ProductID = 1

-- With 2 parameters
CREATE PROCEDURE GetProductsByCategoryAndPrice
	@Category VARCHAR(50),
	@MinPrice DECIMAL(10,2)
AS
BEGIN
	SELECT * FROM PRODUCTS
	WHERE Category = @Category
	AND Price >= @MinPrice;
END;

exec GetProductsByCategoryAndPrice @Category = 'Electronics' ,
@MinPrice = 500.00;


-- Stored Proc with output variable

CREATE PROCEDURE GetTotalProductsInCategory
	@Category VARCHAR(50),
	@TotalProducts INT OUTPUT
AS
BEGIN
	SELECT @TotalProducts = COUNT(*) FROM Products
	where Category = @Category;
END;

DECLARE @TOTAL INT;
EXEC GetTotalProductsInCategory @Category = 'Electronics', @TotalProducts = @Total OUTPUT
SELECT @Total AS TotalProductsInCategory

-- Transaction

CREATE PROCEDURE ProcessOrder
	@ProductID INT,
	@Quantity INT,
	@OrderDate DATE
AS
BEGIN
	BEGIN TRANSACTION

	BEGIN TRY
		--Insert Order
		INSERT INTO Orders (ProductID,Quantity,OrderDate)
		VALUES (@ProductID,@Quantity,@OrderDate)

		--Update Stock
		UPDATE Products
		SET StockQuantity = StockQuantity - @Quantity 
		WHERE ProductID = @ProductID

		COMMIT TRANSACTION
	END TRY
	BEGIN CATCH
		ROLLBACK TRANSACTION
		--Handle errors
		THROW
	END CATCH
END;

-- Another example

CREATE PROCEDURE AdjustStock
	@ProductID INT,
	@Adjustment INT
AS
BEGIN
	IF @Adjustment > 0
	BEGIN
		--Add to stock
		UPDATE Products
		SET StockQuantity = StockQuantity + @Adjustment
		WHERE ProductID = @ProductID
	END
	ELSE IF @Adjustment < 0
	BEGIN
		--Subtract from Stock
		UPDATE Products
		SET StockQuantity = StockQuantity + @Adjustment
		WHERE ProductID = @ProductID
	END
END;

EXEC AdjustStock @ProductID = 1, @Adjustment = 5;
EXEC AdjustStock @ProductID = 1, @Adjustment = -3;
