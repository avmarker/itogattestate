--  Список всех заказов за последние 7 дней с именем покупателя и описанием товара
SELECT o.id AS заказ_id, c.first_name, c.last_name, p.description AS товар, o.order_date
FROM public."order" o
JOIN customer c ON o.customer_id = c.id
JOIN product p ON o.product_id = p.id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '7 days';
-- Топ-3 самых популярных товаров по заказам
SELECT p.description, SUM(o.quantity) AS итог_заказов
FROM public."order" o
JOIN product p ON o.product_id = p.id
GROUP BY p.description
ORDER BY итог_заказов DESC
LIMIT 3;
--  Обновление количества на складе при покупке (уменьшить количество для товара "Яблоки")
UPDATE product SET quantity = quantity - 2 WHERE description='Яблоки';
-- Обновление статуса заказа (например, с "Обработка" на "Доставлено" для заказа с ID=2)
UPDATE public."order"
SET status_id = (SELECT id FROM order_status WHERE name='Доставлено')
WHERE id = 2;
-- Вставка нового клиента, если его еще нет (по телефону)
INSERT INTO customer (first_name, last_name, phone, email)
SELECT 'Алекс', 'Петухов', '89998887777', 'alex.petux@mail.com'
WHERE NOT EXISTS (SELECT 1 FROM customer WHERE phone='89998887777');
SELECT * FROM customer;
-- Создать заказ для этого клиента и товара "Кофе растворимый" (если еще не существует)
WITH cte AS (
  SELECT id FROM customer WHERE phone='89998887777'
)
INSERT INTO public."order" (product_id, customer_id, order_date, quantity, status_id)
SELECT
  (SELECT id FROM product WHERE description='Кофе растворимый'),
  (SELECT id FROM cte),
  CURRENT_TIMESTAMP,
  2,
  (SELECT id FROM order_status WHERE name='Обработка')
WHERE
  (SELECT COUNT(*) FROM public."order"
     WHERE product_id=(SELECT id FROM product WHERE description='Кофе растворимый')
       AND customer_id=(SELECT id FROM cte)
       AND quantity=2) = 0;
       -- Вывод всех заказов для клиента с телефоном 89991112233
SELECT o.id, c.first_name, c.last_name, p.description, o.order_date, o.quantity, s.name AS status
FROM public."order" o
JOIN customer c ON o.customer_id = c.id
JOIN product p ON o.product_id = p.id
JOIN order_status s ON o.status_id = s.id
WHERE c.phone='89998887777'
ORDER BY o.order_date DESC;
-- Удаление клиентов без заказов
DELETE FROM customer
WHERE id NOT IN (SELECT DISTINCT customer_id FROM public."order");