-----------------------------------------------------------------------------------------------------
-- вывести количество фильмов в каждой категории, отсортировать по убыванию.
-----------------------------------------------------------------------------------------------------
   SELECT t1.category_id,
          t1.name,
          COUNT(1) AS "film_count"
     FROM category      t1
LEFT JOIN film_category t2 ON t2.category_id = t1.category_id
 GROUP BY t1.category_id,
          t1.name
 ORDER BY film_count DESC
;

-----------------------------------------------------------------------------------------------------
-- вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
-----------------------------------------------------------------------------------------------------
SELECT t1.actor_id,
       t1.first_name,
       t1.last_name,
       SUM(t3.film_rental_count) AS "actor_rental_count"
  FROM actor      t1
  JOIN film_actor t2 ON t2.actor_id = t1.actor_id
  JOIN (SELECT s1.film_id,
               COUNT(1) AS "film_rental_count"
          FROM inventory s1
          JOIN rental    s2 ON s2.inventory_id = s1.inventory_id
        GROUP BY s1.film_id) t3 ON t3.film_id = t2.film_id
GROUP BY t1.actor_id,
         t1.first_name,
         t1.last_name
ORDER BY actor_rental_count DESC
LIMIT 10
;

-----------------------------------------------------------------------------------------------------
-- вывести категорию фильмов, на которую потратили больше всего денег.
-----------------------------------------------------------------------------------------------------
SELECT t1.category_id,
       t1.name,
       SUM(t3.film_rental_amount) AS "category_rental_amount"
  FROM category      t1
  JOIN film_category t2 ON t2.category_id = t1.category_id
  JOIN (SELECT s1.film_id,
               SUM(s3.amount) AS "film_rental_amount"
          FROM inventory s1
          JOIN rental    s2 ON s2.inventory_id = s1.inventory_id
          JOIN payment   s3 ON s3.rental_id    = s2.rental_id
        GROUP BY s1.film_id) t3 ON t3.film_id = t2.film_id
GROUP BY t1.category_id,
         t1.name
ORDER BY category_rental_amount DESC
LIMIT 1
;

-----------------------------------------------------------------------------------------------------
-- вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
-----------------------------------------------------------------------------------------------------
   SELECT t1.title
     FROM film t1
LEFT JOIN inventory t2 on t2.film_id = t1.film_id
    WHERE t2.inventory_id is null
;

-----------------------------------------------------------------------------------------------------
-- вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
-- Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
-----------------------------------------------------------------------------------------------------
WITH children_film_actor as (
     SELECT t1.actor_id,
            count(1) AS "actor_film_count"
       FROM actor         t1
       JOIN film_actor    t2 ON t2.actor_id    = t1.actor_id
       JOIN film_category t3 ON t3.film_id     = t2.film_id
       JOIN category      t4 ON t4.category_id = t3.category_id
                            AND t4.name        = 'Children'
     GROUP BY t1.actor_id
)
SELECT t1.actor_id,
       t1.first_name,
       t1.last_name,
       t2.actor_film_count
  FROM actor               t1
  JOIN children_film_actor t2 ON t2.actor_id = t1.actor_id
 WHERE t2.actor_film_count IN (SELECT actor_film_count
                                 FROM children_film_actor
                               ORDER BY actor_film_count DESC
                               LIMIT 3)
ORDER BY t2.actor_film_count DESC
;

-----------------------------------------------------------------------------------------------------
-- вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
-- Отсортировать по количеству неактивных клиентов по убыванию.
-----------------------------------------------------------------------------------------------------
   SELECT t1.city_id,
          t1.city,
          SUM(CASE WHEN t3.active  = 1 THEN 1 ELSE 0 END) AS "active_count",
          SUM(CASE WHEN t3.active <> 1 THEN 1 ELSE 0 END) AS "not_active_count"
     FROM city     t1
LEFT JOIN address  t2 on t2.city_id    = t1.city_id
LEFT JOIN customer t3 on t3.address_id = t2.address_id
 GROUP BY t1.city_id,
          t1.city
 ORDER BY not_active_count DESC
;

-----------------------------------------------------------------------------------------------------
-- вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах
-- (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для
-- городов в которых есть символ “-”. Написать все в одном запросе.
-----------------------------------------------------------------------------------------------------
SELECT *
  FROM (SELECT t1.category_id,
               t1.name,
               SUM(t3.film_rental_time) AS "category_rental_time",
               'City starts with A'     AS "condition_type"
          FROM category      t1
          JOIN film_category t2 ON t2.category_id = t1.category_id
          JOIN (SELECT s1.film_id,
                       SUM(s2.return_date - s2.rental_date) AS "film_rental_time"
                  FROM inventory s1
                  JOIN rental    s2 ON s2.inventory_id = s1.inventory_id
                  JOIN customer  s3 ON s3.customer_id  = s2.customer_id
                  JOIN address   s4 ON s4.address_id   = s3.address_id
                  JOIN city      s5 ON s5.city_id      = s4.city_id
                                   AND s5.city ILIKE 'a%'
                GROUP BY s1.film_id) t3 on t3.film_id = t2.film_id
        GROUP BY t1.category_id,
                 t1.name
        ORDER BY category_rental_time DESC
        LIMIT 1) as A
UNION
SELECT *
  FROM (SELECT t1.category_id,
               t1.name,
               SUM(t3.film_rental_time) AS "category_rental_time",
               'City contains -'        AS "condition_type"
          FROM category      t1
          JOIN film_category t2 ON t2.category_id = t1.category_id
          JOIN (SELECT s1.film_id,
                       SUM(s2.return_date - s2.rental_date) AS "film_rental_time"
                  FROM inventory s1
                  JOIN rental    s2 ON s2.inventory_id = s1.inventory_id
                  JOIN customer  s3 ON s3.customer_id  = s2.customer_id
                  JOIN address   s4 ON s4.address_id   = s3.address_id
                  JOIN city      s5 ON s5.city_id      = s4.city_id
                                   AND s5.city LIKE '%-%'
                GROUP BY s1.film_id) t3 on t3.film_id = t2.film_id
        GROUP BY t1.category_id,
                 t1.name
        ORDER BY category_rental_time DESC
        LIMIT 1) as B
;
