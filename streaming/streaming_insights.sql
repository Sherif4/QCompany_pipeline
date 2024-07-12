-- Select the date of the view, total number of views per product per day
SELECT 
    DATE(timestamp) AS viewDate, 
    COUNT(*) AS totalViews,
    productId
FROM 
    product_view
GROUP BY 
    DATE(timestamp), 
    productId;

-- Select the date, total quantity added to cart per day
SELECT 
    DATE(timestamp) AS cartDate, 
    SUM(quantity) AS totalQuantityAdded
FROM 
    total_add_cart_per_day
GROUP BY 
    DATE(timestamp);

-- Select the date, total number of clicks per day
SELECT 
    DATE(timestamp) AS clickDate, 
    COUNT(*) AS totalClicks
FROM 
    total_number_per_day
GROUP BY 
    DATE(timestamp);

-- Select the algorithm used for recommendation, total number of clicks per algorithm
SELECT 
    algorithm, 
    COUNT(*) AS totalClicks
FROM 
    recommendation_per_algrothim
GROUP BY 
    algorithm;

-- Select the top 5 customers based on their total purchase amount
SELECT 
    customerId, 
    SUM(totalAmount) AS totalPurchaseAmount
FROM 
    topfivecustomer
GROUP BY 
    customerId
ORDER BY 
    totalPurchaseAmount DESC
LIMIT 5;

-- Select the date of sale, total quantity sold, and total sales amount per day
SELECT 
    DATE(timestamp) AS saleDate, 
    SUM(quantity) AS totalQuantitySold,
    SUM(totalAmount) AS totalSalesAmount
FROM 
    purchases
GROUP BY 
    DATE(timestamp);