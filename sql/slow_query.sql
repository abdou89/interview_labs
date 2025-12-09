-- =============================================================================
-- Requêtes SQL avec problèmes de performance à identifier par le candidat
-- Contexte: Base de données avec tables Orders (500M+ lignes), Customers (10M+ lignes)
-- =============================================================================

-- =============================================================================
-- REQUETE 1: Rapport des commandes récentes par client actif
-- =============================================================================
SELECT *
FROM Orders o
JOIN Customers c ON c.CustomerId = o.CustomerId
WHERE YEAR(o.OrderDate) = 2024
  AND MONTH(o.OrderDate) >= 10
  AND c.Status = 'ACTIVE'
ORDER BY o.OrderDate DESC;
 
-- =============================================================================
-- REQUETE 3: Agrégation sur table volumineuse
-- =============================================================================
SELECT 
    c.CustomerId,
    c.Name,
    dbo.fn_GetCustomerCategory(c.CustomerId) AS Category,
    COUNT(o.OrderId) AS TotalOrders,
    SUM(o.Amount) AS TotalAmount,
    AVG(o.Amount) AS AvgOrderAmount
FROM Customers c
LEFT JOIN Orders o ON o.CustomerId = c.CustomerId
GROUP BY c.CustomerId, c.Name
HAVING COUNT(o.OrderId) > 10
ORDER BY TotalAmount DESC;

-- =============================================================================
-- REQUETE 4: Sous-requête corrélée (N+1 en SQL)
-- =============================================================================
SELECT 
    o.OrderId,
    o.CustomerId,
    o.Amount,
    (SELECT TOP 1 o2.Amount 
     FROM Orders o2 
     WHERE o2.CustomerId = o.CustomerId 
       AND o2.OrderDate < o.OrderDate 
     ORDER BY o2.OrderDate DESC) AS PreviousOrderAmount,
    (SELECT AVG(o3.Amount) 
     FROM Orders o3 
     WHERE o3.CustomerId = o.CustomerId) AS CustomerAvgAmount
FROM Orders o
WHERE o.OrderDate >= '2024-01-01';

-- =============================================================================
-- REQUETE 5: Transaction de mise à jour massive
-- =============================================================================
UPDATE Orders
SET Status = 'ARCHIVED',
    ModifiedDate = GETDATE(),
    ModifiedBy = 'SYSTEM'
WHERE OrderDate < DATEADD(year, -2, GETDATE())
  AND Status = 'COMPLETED';

-- =============================================================================
-- REQUETE 6: Requête avec OR mal optimisée
-- =============================================================================
SELECT o.OrderId, o.CustomerId, o.OrderDate, o.Amount
FROM Orders o
WHERE o.CustomerId = 12345
   OR o.ShippingAddressId = 67890
   OR o.BillingAddressId = 67890
ORDER BY o.OrderDate DESC;

-- =============================================================================
-- REQUETE 7: CTE 
-- =============================================================================
;WITH OrderHierarchy AS (
    SELECT OrderId, ParentOrderId, OrderDate, Amount, 0 AS Level
    FROM Orders
    WHERE ParentOrderId IS NULL
    
    UNION ALL
    
    SELECT o.OrderId, o.ParentOrderId, o.OrderDate, o.Amount, oh.Level + 1
    FROM Orders o
    INNER JOIN OrderHierarchy oh ON o.ParentOrderId = oh.OrderId
)
SELECT * FROM OrderHierarchy;

-- =============================================================================
-- REQUETE 8: Jointure avec table temporaire 

-- =============================================================================
CREATE TABLE #TempCustomerIds (CustomerId INT);

INSERT INTO #TempCustomerIds
SELECT CustomerId FROM Customers WHERE Region = 'EUROPE';

SELECT o.*
FROM Orders o
INNER JOIN #TempCustomerIds t ON o.CustomerId = t.CustomerId
WHERE o.OrderDate >= '2024-01-01';

DROP TABLE #TempCustomerIds;
