{% docs __overview__ %}

# Аналитический слой интернет-магазина

Проект Data Engineering на dbt и Greenplum.

Данные поступают из OLTP-системы интернет-магазина
в сырой слой Greenplum.

Проект состоит из трех слоев:
- staging - подготовка данных;
- intermediate - объединение и преобразование данных;
- marts - аналитические витрины.

Основные витрины:
fct_orders, fct_order_items, dim_customers,
dim_products, dim_dates.

{% enddocs %}

{% docs confirmed_order %}

Подтвержденная покупка - заказ со статусом paid,
для которого существует хотя бы одна успешная
платежная операция, не являющаяся возвратом.

{% enddocs %}