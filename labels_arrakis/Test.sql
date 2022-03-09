-- Databricks notebook source
SELECT 
from as address,
get_labels(from) as labels
FROM jonas10_ethereum.transactions
LIMIT 10
